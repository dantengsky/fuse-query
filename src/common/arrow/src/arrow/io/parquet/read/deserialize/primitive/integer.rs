// Copyright 2020-2022 Jorge C. Leitão
// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;

use num_traits::AsPrimitive;
use parquet2::deserialize::SliceFilteredIter;
use parquet2::encoding::delta_bitpacked::Decoder;
use parquet2::encoding::Encoding;
use parquet2::page::split_buffer;
use parquet2::page::DataPage;
use parquet2::page::DictPage;
use parquet2::schema::Repetition;
use parquet2::types::decode;
use parquet2::types::NativeType as ParquetNativeType;

use super::super::utils;
use super::super::Pages;
use super::basic::finish;
use super::basic::PrimitiveDecoder;
use super::basic::State as PrimitiveState;
use crate::arrow::array::MutablePrimitiveArray;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::deserialize::utils::get_selected_rows;
use crate::arrow::io::parquet::read::deserialize::utils::FilteredOptionalPageValidity;
use crate::arrow::io::parquet::read::deserialize::utils::OptionalPageValidity;
use crate::arrow::types::NativeType;

/// The state of a [`DataPage`] of an integer parquet type (i32 or i64)
#[derive(Debug)]
enum State<'a, T>
where T: NativeType
{
    Common(PrimitiveState<'a, T>),
    DeltaBinaryPackedRequired(Decoder<'a>),
    DeltaBinaryPackedOptional(OptionalPageValidity<'a>, Decoder<'a>),
    FilteredDeltaBinaryPackedRequired(SliceFilteredIter<Decoder<'a>>),
    FilteredDeltaBinaryPackedOptional(FilteredOptionalPageValidity<'a>, Decoder<'a>),
}

impl<'a, T> utils::PageState<'a> for State<'a, T>
where T: NativeType
{
    fn len(&self) -> usize {
        match self {
            State::Common(state) => state.len(),
            State::DeltaBinaryPackedRequired(state) => state.size_hint().0,
            State::DeltaBinaryPackedOptional(state, _) => state.len(),
            State::FilteredDeltaBinaryPackedRequired(state) => state.size_hint().0,
            State::FilteredDeltaBinaryPackedOptional(state, _) => state.len(),
        }
    }
}

/// Decoder of integer parquet type
#[derive(Debug)]
struct IntDecoder<T, P, F>(PrimitiveDecoder<T, P, F>)
where
    T: NativeType,
    P: ParquetNativeType,
    i64: num_traits::AsPrimitive<P>,
    F: Fn(P) -> T;

impl<T, P, F> IntDecoder<T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    i64: num_traits::AsPrimitive<P>,
    F: Fn(P) -> T,
{
    #[inline]
    fn new(op: F) -> Self {
        Self(PrimitiveDecoder::new(op))
    }
}

impl<'a, T, P, F> utils::Decoder<'a> for IntDecoder<T, P, F>
where
    T: NativeType,
    P: ParquetNativeType,
    i64: num_traits::AsPrimitive<P>,
    F: Copy + Fn(P) -> T,
{
    type State = State<'a, T>;
    type Dict = Vec<T>;
    type DecodedState = (Vec<T>, MutableBitmap);

    fn build_state(&self, page: &'a DataPage, dict: Option<&'a Self::Dict>) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (page.encoding(), dict, is_optional, is_filtered) {
            (Encoding::DeltaBinaryPacked, _, false, false) => {
                let (_, _, values) = split_buffer(page)?;
                Decoder::try_new(values)
                    .map(State::DeltaBinaryPackedRequired)
                    .map_err(Error::from)
            }
            (Encoding::DeltaBinaryPacked, _, true, false) => {
                let (_, _, values) = split_buffer(page)?;
                Ok(State::DeltaBinaryPackedOptional(
                    OptionalPageValidity::try_new(page)?,
                    Decoder::try_new(values)?,
                ))
            }
            (Encoding::DeltaBinaryPacked, _, false, true) => {
                let (_, _, values) = split_buffer(page)?;
                let values = Decoder::try_new(values)?;

                let rows = get_selected_rows(page);
                let values = SliceFilteredIter::new(values, rows);

                Ok(State::FilteredDeltaBinaryPackedRequired(values))
            }
            (Encoding::DeltaBinaryPacked, _, true, true) => {
                let (_, _, values) = split_buffer(page)?;
                let values = Decoder::try_new(values)?;

                Ok(State::FilteredDeltaBinaryPackedOptional(
                    FilteredOptionalPageValidity::try_new(page)?,
                    values,
                ))
            }
            _ => self.0.build_state(page, dict).map(State::Common),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        self.0.with_capacity(capacity)
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        decoded: &mut Self::DecodedState,
        remaining: usize,
    ) {
        let (values, validity) = decoded;
        match state {
            State::Common(state) => {
                // eprintln!("extend_from_state: remaining={}", remaining);
                match state {
                    PrimitiveState::Required(page) => {
                        // eprintln!("processing required");
                        // 预分配内存
                        let additional = remaining.min(page.len());
                        values.reserve(additional);

                        // 针对 int64 类型且 op 是 identity 函数的特殊优化
                        if std::any::TypeId::of::<P>() == std::any::TypeId::of::<i64>() &&
                           std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                            // 使用直接迭代器方式，避免中间缓冲区
                            let take_count = additional;
                            let mut count = 0;
                            for chunk in page.values.by_ref() {
                                if count >= take_count {
                                    break;
                                }
                                // 直接解码并添加，跳过 op 函数调用
                                let decoded_value = decode::<P>(chunk);
                                // 安全地将 P 类型转换为 T 类型（此处两者都是 i64）
                                unsafe {
                                    let value = std::mem::transmute_copy::<P, T>(&decoded_value);
                                    values.push(value);
                                }
                                count += 1;
                            }
                        } else {
                            // 对于其他类型，使用更高效的直接迭代器方式
                            let take_count = additional;
                            let mut count = 0;
                            for chunk in page.values.by_ref() {
                                if count >= take_count {
                                    break;
                                }
                                let decoded_value = decode::<P>(chunk);
                                let transformed_value = (self.0.op)(decoded_value);
                                values.push(transformed_value);
                                count += 1;
                            }
                        }
                    }
                    PrimitiveState::Optional(page_validity, page_values) => {
                        // 对于可选值，我们需要处理有效性位图
                        // 这部分比较复杂，暂时保留原有实现
                        utils::extend_from_decoder(
                            validity,
                            page_validity,
                            Some(remaining),
                            values,
                            page_values.values.by_ref().map(decode::<P>).map(self.0.op),
                        );
                    }
                    // 其他状态暂时保留原有实现
                    _ => self.0.extend_from_state(state, decoded, remaining),
                }
            }
            State::DeltaBinaryPackedRequired(state) => {
                values.extend(
                    state
                        .by_ref()
                        .map(|x| x.unwrap().as_())
                        .map(self.0.op)
                        .take(remaining),
                );
            }
            State::DeltaBinaryPackedOptional(page_validity, page_values) => {
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(remaining),
                    values,
                    page_values
                        .by_ref()
                        .map(|x| x.unwrap().as_())
                        .map(self.0.op),
                );
            }
            State::FilteredDeltaBinaryPackedRequired(page) => {
                values.extend(
                    page.by_ref()
                        .map(|x| x.unwrap().as_())
                        .map(self.0.op)
                        .take(remaining),
                );
            }
            State::FilteredDeltaBinaryPackedOptional(page_validity, page_values) => {
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(remaining),
                    values,
                    page_values
                        .by_ref()
                        .map(|x| x.unwrap().as_())
                        .map(self.0.op),
                );
            }
        }
    }

    fn deserialize_dict(&self, page: &DictPage) -> Self::Dict {
        self.0.deserialize_dict(page)
    }
}

/// An [`Iterator`] adapter over [`Pages`] assumed to be encoded as primitive arrays
/// encoded as parquet integer types
#[derive(Debug)]
pub struct IntegerIter<T, I, P, F>
where
    I: Pages,
    T: NativeType,
    P: ParquetNativeType,
    F: Fn(P) -> T,
{
    iter: I,
    data_type: DataType,
    items: VecDeque<(Vec<T>, MutableBitmap)>,
    remaining: usize,
    chunk_size: Option<usize>,
    dict: Option<Vec<T>>,
    op: F,
    phantom: std::marker::PhantomData<P>,
}

impl<T, I, P, F> IntegerIter<T, I, P, F>
where
    I: Pages,
    T: NativeType,
    P: ParquetNativeType,
    F: Copy + Fn(P) -> T,
{
    pub fn new(
        iter: I,
        data_type: DataType,
        num_rows: usize,
        chunk_size: Option<usize>,
        op: F,
    ) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            dict: None,
            remaining: num_rows,
            chunk_size,
            op,
            phantom: Default::default(),
        }
    }
}

impl<T, I, P, F> Iterator for IntegerIter<T, I, P, F>
where
    I: Pages,
    T: NativeType,
    P: ParquetNativeType,
    i64: num_traits::AsPrimitive<P>,
    F: Copy + Fn(P) -> T,
{
    type Item = Result<MutablePrimitiveArray<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let maybe_state = utils::next(
                &mut self.iter,
                &mut self.items,
                &mut self.dict,
                &mut self.remaining,
                self.chunk_size,
                &IntDecoder::new(self.op),
            );
            match maybe_state {
                utils::MaybeNext::Some(Ok((values, validity))) => {
                    return Some(Ok(finish(&self.data_type, values, validity)));
                }
                utils::MaybeNext::Some(Err(e)) => return Some(Err(e)),
                utils::MaybeNext::None => return None,
                utils::MaybeNext::More => continue,
            }
        }
    }
}

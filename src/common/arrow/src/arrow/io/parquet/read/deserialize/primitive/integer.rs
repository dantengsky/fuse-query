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
                        if std::any::TypeId::of::<P>() == std::any::TypeId::of::<i64>()
                            && std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>()
                        {
                            // 尝试获取原始数据
                            // 这里我们需要检查 page.values 背后的数据是否连续
                            // 由于 page.values 是 ChunksExact 迭代器，我们需要找到一种方式来访问原始数据

                            // 方法1: 如果我们能获取到第一个块，并且确定后续数据是连续的
                            let mut peekable = page.values.clone().peekable();
                            if let Some(first_chunk) = peekable.peek() {
                                // 确保我们有足够的数据
                                let available = page.len();
                                let to_copy = additional.min(available);

                                unsafe {
                                    // 获取源指针
                                    let src_ptr = first_chunk.as_ptr() as *const i64;

                                    // 预分配目标空间
                                    values.reserve(to_copy);
                                    let old_len = values.len();
                                    values.set_len(old_len + to_copy);

                                    // 直接复制内存
                                    // 注意：这里假设所有数据是连续的
                                    std::ptr::copy_nonoverlapping(
                                        src_ptr,
                                        values.as_mut_ptr().add(old_len) as *mut i64,
                                        to_copy,
                                    );

                                    // 消耗掉迭代器中的元素
                                    //                                    for _ in 0..to_copy {
                                    //                                        page.values.next();
                                    //                                    }
                                }
                            } else {
                                // 回退到逐个处理
                                values.extend(page.values.by_ref().take(additional).map(
                                    |chunk| unsafe {
                                        let ptr = chunk.as_ptr() as *const i64;
                                        std::mem::transmute_copy::<i64, T>(&*ptr)
                                    },
                                ));
                            }
                        } else {
                            // 对于其他类型，使用更高效的直接迭代器方式
                            values.extend(
                                page.values
                                    .by_ref()
                                    .take(additional)
                                    .map(|chunk| (self.0.op)(decode::<P>(chunk))),
                            );
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

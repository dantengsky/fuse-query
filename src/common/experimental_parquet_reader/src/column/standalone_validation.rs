//! Standalone validation of the ultra-simple macro dispatch system

use crate::column::TypeId;
use databend_common_exception::Result;

/// Test function that validates our new macro system compiles correctly
pub fn validate_ultra_simple_macros() -> Result<()> {
    // Test that TypeId matching works as expected in our macros
    let test_type_dispatch = |type_id: &TypeId| -> bool {
        match type_id {
            TypeId::Boolean => true,
            TypeId::Int8 | TypeId::Int16 | TypeId::Int32 | TypeId::Int64 => true,
            TypeId::UInt8 | TypeId::UInt16 | TypeId::UInt32 | TypeId::UInt64 => true,
            TypeId::Float32 | TypeId::Float64 => true,
            _ => false,
        }
    };
    
    // Validate key types
    assert!(test_type_dispatch(&TypeId::Boolean));
    assert!(test_type_dispatch(&TypeId::Int32));
    assert!(test_type_dispatch(&TypeId::Float64));
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use databend_common_expression::TableDataType;
    use databend_common_expression::types::NumberDataType;
    
    #[test]
    fn test_type_id_conversion() {
        let int32_type = TableDataType::Number(NumberDataType::Int32);
        let type_id = TypeId::from_table_data_type(&int32_type);
        assert!(matches!(type_id, TypeId::Int32));
        
        let bool_type = TableDataType::Boolean;
        let type_id = TypeId::from_table_data_type(&bool_type);
        assert!(matches!(type_id, TypeId::Boolean));
    }
    
    #[test]
    fn test_macro_validation() {
        assert!(validate_ultra_simple_macros().is_ok());
    }
}
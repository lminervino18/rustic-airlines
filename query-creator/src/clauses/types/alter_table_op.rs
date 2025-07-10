use super::column::Column;
use super::datatype::DataType;

#[derive(Debug, Clone)]
pub enum AlterTableOperation {
    AddColumn(Column),
    DropColumn(String),
    ModifyColumn(String, DataType, bool), // column name, new data type, allows null
    RenameColumn(String, String),         // old column name, new column name
}

// Implementación de `PartialEq` para permitir comparación de `AlterTableOperation`
impl PartialEq for AlterTableOperation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (AlterTableOperation::AddColumn(col1), AlterTableOperation::AddColumn(col2)) => {
                col1 == col2
            }
            (AlterTableOperation::DropColumn(name1), AlterTableOperation::DropColumn(name2)) => {
                name1 == name2
            }
            (
                AlterTableOperation::ModifyColumn(name1, dtype1, null1),
                AlterTableOperation::ModifyColumn(name2, dtype2, null2),
            ) => name1 == name2 && dtype1 == dtype2 && null1 == null2,
            (
                AlterTableOperation::RenameColumn(old1, new1),
                AlterTableOperation::RenameColumn(old2, new2),
            ) => old1 == old2 && new1 == new2,
            _ => false,
        }
    }
}

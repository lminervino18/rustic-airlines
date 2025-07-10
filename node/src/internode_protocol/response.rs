//! TODO: Add documentation

use super::{message::InternodeMessageError, InternodeSerializable};
use std::io::{Cursor, Read};

/// The status of a response sent by a node in response of a coordinator query.
/// It can be either `Ok` or `Error`.
#[derive(Debug, PartialEq, Clone)]
pub enum InternodeResponseStatus {
    Ok = 0x00,
    Error = 0x01,
}

/// The content of a response sent by a node in response of a coordinator query.
///
/// ### Fields
/// - `columns`: The columns of the response.
/// - `select_columns`: The columns of the response that were selected.
/// - `values`: The values of the response.
#[derive(Debug, PartialEq, Clone)]
pub struct InternodeResponseContent {
    pub columns: Vec<String>,
    pub select_columns: Vec<String>,
    pub values: Vec<Vec<String>>,
}

impl InternodeSerializable for InternodeResponseContent {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |   columns_len     |
    /// +----+----+----+----+
    /// |   column1_len     |
    /// +----+----+----+----+
    /// |     column1       |
    /// +----+----+----+----+
    /// |       ...         |
    /// +----+----+----+----+
    /// |   columnN_len     |
    /// +----+----+----+----+
    /// |     columnN       |
    /// +----+----+----+----+
    /// | select_columns_len|
    /// +----+----+----+----+
    /// | select_column1_len|
    /// +----+----+----+----+
    /// |   select_column1  |
    /// +----+----+----+----+
    /// |       ...         |
    /// +----+----+----+----+
    /// | select_columnN_len|
    /// +----+----+----+----+
    /// |   select_columnN  |
    /// +----+----+----+----+
    /// |     values_len    |
    /// +----+----+----+----+
    /// |     value1_len    |
    /// +----+----+----+----+
    /// |    value1_part1   |
    /// +----+----+----+----+
    /// |       ...         |
    /// +----+----+----+----+
    /// |    value1_partN   |
    /// +----+----+----+----+
    /// |       ...         |
    /// +----+----+----+----+
    /// |     valueN_len    |
    /// +----+----+----+----+
    /// |    valueN_part1   |
    /// +----+----+----+----+
    /// |       ...         |
    /// +----+----+----+----+
    /// |    valueN_partN   |
    /// +----+----+----+----+
    /// ```
    /// Serializes the `InternodeResponseContent` into a `Vec<u8>`.
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let columns_len = self.columns.len() as u32;
        bytes.extend(&columns_len.to_be_bytes());

        for column in &self.columns {
            let column_len = column.len() as u32;
            bytes.extend(&column_len.to_be_bytes());
            bytes.extend(column.as_bytes());
        }

        let select_columns_len = self.select_columns.len() as u32;
        bytes.extend(&select_columns_len.to_be_bytes());

        for select_column in &self.select_columns {
            let select_column_len = select_column.len() as u32;
            bytes.extend(&select_column_len.to_be_bytes());
            bytes.extend(select_column.as_bytes());
        }

        let values_len = self.values.len() as u32;
        bytes.extend(&values_len.to_be_bytes());

        for value in &self.values {
            let value_len = value.len() as u32;
            bytes.extend(&value_len.to_be_bytes());
            for value_part in value {
                let value_part_len = value_part.len() as u32;
                bytes.extend(&value_part_len.to_be_bytes());
                bytes.extend(value_part.as_bytes());
            }
        }

        bytes
    }

    /// Deserializes the `InternodeResponseContent` from a slice of `u8`.
    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError> {
        let mut cursor = Cursor::new(bytes);

        let mut columns_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut columns_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let columns_len = u32::from_be_bytes(columns_len_bytes) as usize;

        let mut columns = Vec::with_capacity(columns_len);
        for _ in 0..columns_len {
            let mut column_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut column_len_bytes)
                .map_err(|_| InternodeMessageError)?;
            let column_len = u32::from_be_bytes(column_len_bytes) as usize;

            let mut column_bytes = vec![0u8; column_len];
            cursor
                .read_exact(&mut column_bytes)
                .map_err(|_| InternodeMessageError)?;
            let column = String::from_utf8(column_bytes).map_err(|_| InternodeMessageError)?;

            columns.push(column);
        }

        let mut select_columns_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut select_columns_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let select_columns_len = u32::from_be_bytes(select_columns_len_bytes) as usize;

        let mut select_columns = Vec::with_capacity(select_columns_len);
        for _ in 0..select_columns_len {
            let mut select_column_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut select_column_len_bytes)
                .map_err(|_| InternodeMessageError)?;
            let select_column_len = u32::from_be_bytes(select_column_len_bytes) as usize;

            let mut select_column_bytes = vec![0u8; select_column_len];
            cursor
                .read_exact(&mut select_column_bytes)
                .map_err(|_| InternodeMessageError)?;
            let select_column =
                String::from_utf8(select_column_bytes).map_err(|_| InternodeMessageError)?;

            select_columns.push(select_column);
        }

        let mut values_len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut values_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let values_len = u32::from_be_bytes(values_len_bytes) as usize;

        let mut values = Vec::with_capacity(values_len);

        for _ in 0..values_len {
            let mut value_len_bytes = [0u8; 4];
            cursor
                .read_exact(&mut value_len_bytes)
                .map_err(|_| InternodeMessageError)?;
            let value_len = u32::from_be_bytes(value_len_bytes) as usize;

            let mut value = Vec::with_capacity(value_len);
            for _ in 0..value_len {
                let mut value_part_len_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut value_part_len_bytes)
                    .map_err(|_| InternodeMessageError)?;
                let value_part_len = u32::from_be_bytes(value_part_len_bytes) as usize;

                let mut value_part_bytes = vec![0u8; value_part_len];
                cursor
                    .read_exact(&mut value_part_bytes)
                    .map_err(|_| InternodeMessageError)?;
                let value_part =
                    String::from_utf8(value_part_bytes).map_err(|_| InternodeMessageError)?;

                value.push(value_part);
            }

            values.push(value);
        }

        Ok(InternodeResponseContent {
            columns,
            select_columns,
            values,
        })
    }
}

/// A response sent by a node in response of a coordinator query.
///
/// ### Fields
/// - `open_query_id`: The `id` of the query to be identified by the open queries handler.
/// - `status`: If the query was successful.
/// - `content`: The response content, if any (for example a `SELECT`). It can be `None`.
#[derive(Debug, PartialEq, Clone)]
pub struct InternodeResponse {
    /// The `id` of the query to be identified by the open queries handler.
    pub open_query_id: u32,
    /// If the query was successful.
    pub status: InternodeResponseStatus,
    /// The response content, if any (for example a `SELECT`).
    pub content: Option<InternodeResponseContent>,
}

impl InternodeResponse {
    /// Creates a new `InternodeResponse`.
    pub fn new(
        open_query_id: u32,
        status: InternodeResponseStatus,
        content: Option<InternodeResponseContent>,
    ) -> Self {
        Self {
            open_query_id,
            status,
            content,
        }
    }
}

impl InternodeSerializable for InternodeResponse {
    /// ```md
    /// 0    8    16   24   32
    /// +----+----+----+----+
    /// |   open_query_id   |
    /// +----+----+----+----+
    /// |stat|cont_len |cont|
    /// +----+----+----+----+
    /// |      content      |
    /// |        ...        |
    /// |      content      |
    /// +----+----+----+----+
    /// ```
    /// Serializes the `InternodeResponse` into a `Vec<u8>`.
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Serializa el ID de la query abierta
        bytes.extend(&self.open_query_id.to_be_bytes());

        // Serializa el estado
        let status_byte = match self.status {
            InternodeResponseStatus::Ok => 0x00,
            InternodeResponseStatus::Error => 0x01,
        };
        bytes.push(status_byte);

        // Serializa el contenido
        if let Some(content) = &self.content {
            let content_bytes = content.as_bytes();
            bytes.extend((content_bytes.len() as u16).to_be_bytes()); // Longitud del contenido
            bytes.extend(content_bytes); // Contenido
        } else {
            bytes.extend(0u16.to_be_bytes()); // Longitud del contenido = 0
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, InternodeMessageError> {
        let mut cursor = Cursor::new(bytes);

        // Deserializa el ID de la query abierta
        let mut open_query_id_bytes = [0u8; 4];
        cursor
            .read_exact(&mut open_query_id_bytes)
            .map_err(|_| InternodeMessageError)?;
        let open_query_id = u32::from_be_bytes(open_query_id_bytes);

        // Deserializa el estado
        let mut status_byte = [0u8; 1];
        cursor
            .read_exact(&mut status_byte)
            .map_err(|_| InternodeMessageError)?;
        let status = match status_byte[0] {
            0x00 => InternodeResponseStatus::Ok,
            0x01 => InternodeResponseStatus::Error,
            _ => return Err(InternodeMessageError),
        };

        // Deserializa el contenido
        let mut content_len_bytes = [0u8; 2];
        cursor
            .read_exact(&mut content_len_bytes)
            .map_err(|_| InternodeMessageError)?;
        let content_len = u16::from_be_bytes(content_len_bytes);

        let content = if content_len == 0 {
            None
        } else {
            let mut content_bytes = vec![0u8; content_len as usize];
            cursor
                .read_exact(&mut content_bytes)
                .map_err(|_| InternodeMessageError)?;
            Some(
                InternodeResponseContent::from_bytes(&content_bytes)
                    .map_err(|_| InternodeMessageError)?,
            )
        };

        Ok(InternodeResponse {
            open_query_id,
            status,
            content,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_response_to_bytes() {
        let response = InternodeResponse {
            open_query_id: 1,
            status: InternodeResponseStatus::Ok,
            content: Some(InternodeResponseContent {
                columns: vec!["column1".to_string(), "column2".to_string()],
                select_columns: vec!["column1".to_string(), "column2".to_string()],
                values: vec![vec!["value1".to_string(), "value2".to_string()]],
            }),
        };

        let response_bytes = response.as_bytes();

        let mut bytes = Vec::new();

        bytes.extend(response.open_query_id.to_be_bytes());

        let status_byte = match response.status {
            InternodeResponseStatus::Ok => 0x00,
            InternodeResponseStatus::Error => 0x01,
        };
        bytes.push(status_byte);

        let content_bytes = if let Some(content) = response.content {
            Some(content.as_bytes())
        } else {
            None
        };

        if let Some(c_bytes) = content_bytes {
            bytes.extend((c_bytes.len() as u16).to_be_bytes());
            bytes.extend(&c_bytes);
        } else {
            bytes.push(0);
        }

        assert_eq!(response_bytes, bytes);
    }

    #[test]
    fn test_response_from_bytes() {
        let response = InternodeResponse {
            open_query_id: 1,
            status: InternodeResponseStatus::Ok,
            content: Some(InternodeResponseContent {
                columns: vec!["column1".to_string(), "column2".to_string()],
                select_columns: vec!["column1".to_string(), "column2".to_string()],
                values: vec![vec!["value1".to_string(), "value2".to_string()]],
            }),
        };

        let response_bytes = response.as_bytes();

        let parsed_response = InternodeResponse::from_bytes(&response_bytes).unwrap();

        assert_eq!(parsed_response, response);
    }

    #[test]
    fn test_content_to_bytes() {
        let content = InternodeResponseContent {
            columns: vec!["column1".to_string(), "column2".to_string()],
            select_columns: vec!["column1".to_string(), "column2".to_string()],
            values: vec![vec!["value1".to_string(), "value2".to_string()]],
        };

        let content_bytes = content.as_bytes();

        let mut bytes = Vec::new();

        let columns_len = content.columns.len() as u32;
        bytes.extend(&columns_len.to_be_bytes());

        for column in &content.columns {
            let column_len = column.len() as u32;
            bytes.extend(&column_len.to_be_bytes());
            bytes.extend(column.as_bytes());
        }

        let select_columns_len = content.select_columns.len() as u32;
        bytes.extend(&select_columns_len.to_be_bytes());

        for select_column in &content.select_columns {
            let select_column_len = select_column.len() as u32;
            bytes.extend(&select_column_len.to_be_bytes());
            bytes.extend(select_column.as_bytes());
        }

        let values_len = content.values.len() as u32;
        bytes.extend(&values_len.to_be_bytes());

        for value in &content.values {
            let value_len = value.len() as u32;
            bytes.extend(&value_len.to_be_bytes());
            for value_part in value {
                let value_part_len = value_part.len() as u32;
                bytes.extend(&value_part_len.to_be_bytes());
                bytes.extend(value_part.as_bytes());
            }
        }

        assert_eq!(content_bytes, bytes);
    }

    #[test]
    fn test_content_from_bytes() {
        let content = InternodeResponseContent {
            columns: vec!["column1".to_string(), "column2".to_string()],
            select_columns: vec!["column1".to_string(), "column2".to_string()],
            values: vec![vec!["value1".to_string(), "value2".to_string()]],
        };

        let content_bytes = content.as_bytes();

        let parsed_content = InternodeResponseContent::from_bytes(&content_bytes).unwrap();

        assert_eq!(parsed_content, content);
    }

    #[test]
    fn test_content_from_bytes_error() {
        let content_bytes = vec![0, 0, 0, 0, 0];

        let parsed_content = InternodeResponseContent::from_bytes(&content_bytes);

        assert!(parsed_content.is_err());
    }

    #[test]
    fn test_response_with_none_content_to_bytes() {
        let response = InternodeResponse {
            open_query_id: 1,
            status: InternodeResponseStatus::Ok,
            content: None,
        };

        let response_bytes = response.as_bytes();

        let mut bytes = Vec::new();

        bytes.extend(response.open_query_id.to_be_bytes());

        let status_byte = match response.status {
            InternodeResponseStatus::Ok => 0x00,
            InternodeResponseStatus::Error => 0x01,
        };
        bytes.push(status_byte);

        // No content
        bytes.extend(0u16.to_be_bytes()); // Content length = 0

        assert_eq!(response_bytes, bytes);
    }

    #[test]
    fn test_response_with_none_content_from_bytes() {
        let response = InternodeResponse {
            open_query_id: 1,
            status: InternodeResponseStatus::Ok,
            content: None,
        };

        let response_bytes = response.as_bytes();

        let parsed_response = InternodeResponse::from_bytes(&response_bytes).unwrap();

        assert_eq!(parsed_response, response);
    }
}

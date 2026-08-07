#![no_main]

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use libfuzzer_sys::fuzz_target;
use plenora_database_core::field_contract::{validate_schema_contract, FieldContract};
use plenora_database_core::protocol;
use std::collections::HashMap;

/// Chiavi che il contratto giudica, più due chiavi estranee usate come
/// controllo negativo.
const KEYS: [&str; 19] = [
    protocol::GEOMETRY_ENCODING,
    protocol::GEOMETRY_DIMENSIONS,
    protocol::GEOMETRY_TYPES,
    protocol::GEOMETRY_TYPES_DECLARATION,
    protocol::GEOMETRY_SRID,
    protocol::GEOMETRY_CRS_RESOLUTION,
    protocol::GEOMETRY_CRS_ID,
    protocol::GEOMETRY_CRS_DEFINITION,
    protocol::GEOMETRY_CRS_DEFINITION_FORMAT,
    protocol::GEOMETRY_AXIS_ORDER,
    protocol::GEOMETRY_SPATIAL_SEMANTICS,
    protocol::GEOMETRY_PRECISION,
    protocol::FIELD_ID,
    protocol::GEOARROW_EXTENSION_NAME,
    "plenora.dimensions",
    "plenora.srid",
    "plenora.spatial_semantics",
    "plenora.geometry_type",
    "plenora.non.canonica",
];

const TYPES: [DataType; 8] = [
    DataType::Binary,
    DataType::LargeBinary,
    DataType::Utf8,
    DataType::Int32,
    DataType::Int64,
    DataType::Float64,
    DataType::Boolean,
    DataType::Time64(TimeUnit::Microsecond),
];

/// Cursore sui byte non fidati; ogni lettura oltre il buffer restituisce zero.
struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn byte(&mut self) -> u8 {
        let value = self.bytes.get(self.position).copied().unwrap_or_default();
        self.position = self.position.saturating_add(1);
        value
    }

    fn exhausted(&self) -> bool {
        self.position >= self.bytes.len()
    }

    fn text(&mut self) -> String {
        let length = usize::from(self.byte()) % 32;
        let start = self.position.min(self.bytes.len());
        let end = start.saturating_add(length).min(self.bytes.len());
        self.position = end;
        String::from_utf8_lossy(&self.bytes[start..end]).into_owned()
    }
}

fuzz_target!(|input: &[u8]| {
    let mut cursor = Cursor::new(input);
    let field_count = (usize::from(cursor.byte()) % 4) + 1;
    let mut fields = Vec::with_capacity(field_count);

    for index in 0..field_count {
        let data_type = TYPES[usize::from(cursor.byte()) % TYPES.len()].clone();
        let nullable = cursor.byte() % 2 == 0;
        let name = format!("c{index}");

        let entry_count = usize::from(cursor.byte()) % 8;
        let mut metadata = HashMap::new();
        for _ in 0..entry_count {
            if cursor.exhausted() {
                break;
            }
            let key = KEYS[usize::from(cursor.byte()) % KEYS.len()];
            // Metà delle volte il valore è uno di quelli canonici, così il
            // fuzzer raggiunge anche i rami di accettazione.
            let value = if cursor.byte() % 2 == 0 {
                cursor.text()
            } else {
                ["wkb", "ewkb", "xy", "xyzm", "unknown", "geometry", "4326"]
                    [usize::from(cursor.byte()) % 7]
                    .to_owned()
            };
            metadata.insert(key.to_owned(), value);
        }

        fields.push(Field::new(name, data_type, nullable).with_metadata(metadata));
    }

    for field in &fields {
        match FieldContract::parse(field) {
            Ok(contract) => {
                // Un campo spatial è sempre binario e non può essere insieme
                // geometry e geography.
                if contract.spatial {
                    assert!(matches!(
                        field.data_type(),
                        DataType::Binary | DataType::LargeBinary
                    ));
                    assert!(!(contract.is_geometry() && contract.is_geography()));
                } else {
                    assert!(!contract.is_geometry());
                    assert!(!contract.is_geography());
                }
                // L'analisi è deterministica e non consuma il campo.
                assert!(FieldContract::parse(field).is_ok());
            }
            Err(error) => {
                assert!(!error.message.is_empty());
                // L'analisi di un campo è offline: non può dichiarare effetti
                // remoti né un identificativo di esecuzione.
                assert!(error.execution_id.is_none());
            }
        }
    }

    let versions = [None, Some(protocol::CONTRACT_VERSION), Some("2")];
    let selected = versions[usize::from(cursor.byte()) % versions.len()];
    let schema_metadata = selected.map_or_else(HashMap::new, |version| {
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            version.to_owned(),
        )])
    });
    let schema = Schema::new_with_metadata(fields, schema_metadata);

    if validate_schema_contract(&schema).is_ok() {
        // Uno schema accettato ha tutti i campi conformi.
        for field in schema.fields() {
            FieldContract::parse(field).expect("campo accettato dallo schema ma non dal contratto");
        }
        // Una versione dichiarata e diversa da quella supportata non può
        // essere accettata.
        assert!(selected.is_none_or(|version| version == protocol::CONTRACT_VERSION));
    }
});

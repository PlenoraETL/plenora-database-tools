//! LOB del target riletti nella transazione e confrontati con locator verificati.
use super::{OracleWritePlan, Result, Statement, WriteMode};
use crate::transaction::LobVerification;
use std::fmt::Write as _;

pub(super) fn verified_statement(
    plan: &OracleWritePlan,
    statement: &Statement,
) -> Result<(Statement, LobVerification)> {
    let indexes = match plan.mode {
        WriteMode::Update => plan
            .updates
            .iter()
            .chain(&plan.keys)
            .copied()
            .collect::<Vec<_>>(),
        WriteMode::DeleteByKeys => plan.keys.clone(),
        _ => (0..plan.columns.len()).collect(),
    };
    let mut verification = LobVerification::default();
    let mut names = Vec::new();
    let mut variables = Vec::new();
    let mut checks = String::new();
    for (position, index) in indexes.iter().enumerate() {
        let column = &plan.columns[*index];
        let kind = column
            .target_type
            .as_ref()
            .and_then(crate::write_intermediate::TargetType::lob_type);
        verification.types.push(kind);
        if let Some(kind) = kind {
            // Upsert non aggiorna necessariamente tutte le colonne in ingresso.
            if plan.mode == WriteMode::Upsert && !plan.updates.contains(index) {
                return Err(crate::write_intermediate::unsupported());
            }
            let ordinal = position + 1;
            let variable = format!("plenora_lob_{ordinal}");
            let native = if kind == oracle_rs::OracleType::Blob {
                "BLOB"
            } else {
                "CLOB"
            };
            let _ = writeln!(verification.declarations, "{variable} {native};");
            names.push(column.quoted.clone());
            variables.push(variable.clone());
            let _ = write!(
                checks,
                "IF (:{ordinal} IS NULL AND {variable} IS NOT NULL) OR \
                 (:{ordinal} IS NOT NULL AND {variable} IS NULL) THEN \
                 RAISE_APPLICATION_ERROR(-20001, 'LOB verification failed'); END IF; \
                 IF :{ordinal} IS NOT NULL THEN \
                 IF DBMS_LOB.GETLENGTH({variable}) <> DBMS_LOB.GETLENGTH(:{ordinal}) THEN \
                 RAISE_APPLICATION_ERROR(-20001, 'LOB verification failed'); END IF; \
                 plenora_lob_offset := 1; \
                 WHILE plenora_lob_offset <= DBMS_LOB.GETLENGTH({variable}) LOOP \
                 IF NVL(DBMS_LOB.COMPARE({variable}, :{ordinal}, \
                 LEAST(1048576, DBMS_LOB.GETLENGTH({variable}) - plenora_lob_offset + 1), \
                 plenora_lob_offset, plenora_lob_offset), 1) <> 0 THEN \
                 RAISE_APPLICATION_ERROR(-20001, 'LOB verification failed'); END IF; \
                 plenora_lob_offset := plenora_lob_offset + 1048576; END LOOP; END IF;"
            );
        }
    }
    let mut result = statement.clone();
    if names.is_empty() {
        return Ok((result, verification));
    }
    verification
        .declarations
        .push_str("plenora_lob_offset NUMBER;\n");
    let predicate = if plan.mode == WriteMode::Upsert {
        plan.keys
            .iter()
            .map(|index| {
                Ok(format!(
                    "{} = {}",
                    plan.columns[*index].quoted,
                    plan.columns[*index].placeholder(index + 1)?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(" AND ")
    } else {
        verification.declarations.push_str("plenora_rowid ROWID;\n");
        result.sql.push_str(" RETURNING ROWID INTO plenora_rowid");
        "ROWID = plenora_rowid".to_owned()
    };
    verification.after = format!("IF plenora_affected > 0 THEN SELECT {} INTO {} FROM {} WHERE {predicate}; {checks} END IF;",
        names.join(", "), variables.join(", "), plan.target);
    Ok((result, verification))
}

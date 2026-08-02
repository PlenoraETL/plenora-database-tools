use crate::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EwkbStats {
    pub components: u64,
    pub max_depth: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EwkbGeometryMetadata {
    pub base_type: u32,
    pub has_z: bool,
    pub has_m: bool,
    pub srid: Option<u32>,
}

impl EwkbGeometryMetadata {
    #[must_use]
    pub const fn dimensions_label(self) -> &'static str {
        match (self.has_z, self.has_m) {
            (false, false) => "xy",
            (true, false) => "xyz",
            (false, true) => "xym",
            (true, true) => "xyzm",
        }
    }

    #[must_use]
    pub const fn geometry_type_name(self) -> Option<&'static str> {
        match self.base_type {
            1 => Some("Point"),
            2 => Some("LineString"),
            3 => Some("Polygon"),
            4 => Some("MultiPoint"),
            5 => Some("MultiLineString"),
            6 => Some("MultiPolygon"),
            7 => Some("GeometryCollection"),
            8 => Some("CircularString"),
            9 => Some("CompoundCurve"),
            10 => Some("CurvePolygon"),
            11 => Some("MultiCurve"),
            12 => Some("MultiSurface"),
            13 => Some("Curve"),
            14 => Some("Surface"),
            15 => Some("PolyhedralSurface"),
            16 => Some("TIN"),
            17 => Some("Triangle"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EwkbInspection {
    pub stats: EwkbStats,
    pub root: EwkbGeometryMetadata,
    pub has_any_z: bool,
    pub has_any_m: bool,
    pub has_any_embedded_srid: bool,
}

#[derive(Debug, Clone, Copy)]
struct Header {
    base_type: u32,
    dimensions: u64,
    little_endian: bool,
    has_z: bool,
    has_m: bool,
    srid: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
struct Frame {
    remaining: u64,
    child_depth: u64,
}

struct Scanner<'a> {
    bytes: &'a [u8],
    offset: usize,
    components: u64,
    max_components: u64,
    max_depth: u64,
    observed_depth: u64,
    has_any_z: bool,
    has_any_m: bool,
    has_any_embedded_srid: bool,
}

impl Scanner<'_> {
    fn add_components(&mut self, amount: u64) -> Result<()> {
        self.components = self
            .components
            .checked_add(amount)
            .ok_or_else(|| resource_error("overflow nel conteggio componenti EWKB"))?;
        if self.components > self.max_components {
            return Err(resource_error(
                "geometria EWKB oltre il limite di componenti",
            ));
        }
        Ok(())
    }

    fn take(&mut self, amount: usize) -> Result<&[u8]> {
        let end = self
            .offset
            .checked_add(amount)
            .ok_or_else(|| mapping_error("offset EWKB oltre usize"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| mapping_error("payload EWKB troncato"))?;
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self, little_endian: bool) -> Result<u32> {
        let raw: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| mapping_error("intero EWKB troncato"))?;
        Ok(if little_endian {
            u32::from_le_bytes(raw)
        } else {
            u32::from_be_bytes(raw)
        })
    }

    fn header(&mut self) -> Result<Header> {
        let little_endian = match self.take(1)?[0] {
            0 => false,
            1 => true,
            _ => return Err(mapping_error("byte order EWKB non valido")),
        };
        let type_word = self.u32(little_endian)?;
        if type_word & 0x1000_0000 != 0 {
            return Err(mapping_error("bounding box EWKB embedded non supportato"));
        }
        let ewkb_z = type_word & 0x8000_0000 != 0;
        let ewkb_m = type_word & 0x4000_0000 != 0;
        let has_srid = type_word & 0x2000_0000 != 0;
        let mut base_type = type_word & 0x0fff_ffff;
        let (iso_z, iso_m) = if base_type >= 3_000 {
            base_type -= 3_000;
            (true, true)
        } else if base_type >= 2_000 {
            base_type -= 2_000;
            (false, true)
        } else if base_type >= 1_000 {
            base_type -= 1_000;
            (true, false)
        } else {
            (false, false)
        };
        let has_z = ewkb_z || iso_z;
        let has_m = ewkb_m || iso_m;
        let srid = has_srid.then(|| self.u32(little_endian)).transpose()?;
        Ok(Header {
            base_type,
            dimensions: 2 + u64::from(has_z) + u64::from(has_m),
            little_endian,
            has_z,
            has_m,
            srid,
        })
    }

    fn coordinates(&mut self, points: u64, dimensions: u64) -> Result<()> {
        self.add_components(points)?;
        let bytes = points
            .checked_mul(dimensions)
            .and_then(|value| value.checked_mul(8))
            .ok_or_else(|| resource_error("overflow nella dimensione coordinate EWKB"))?;
        self.take(
            usize::try_from(bytes).map_err(|_| resource_error("coordinate EWKB oltre usize"))?,
        )?;
        Ok(())
    }

    fn geometry(&mut self, depth: u64) -> Result<(u64, Header)> {
        if depth == 0 || depth > self.max_depth {
            return Err(resource_error(
                "geometria EWKB oltre il limite di profondità",
            ));
        }
        self.observed_depth = self.observed_depth.max(depth);
        self.add_components(1)?;
        let header = self.header()?;
        self.has_any_z |= header.has_z;
        self.has_any_m |= header.has_m;
        self.has_any_embedded_srid |= header.srid.is_some();
        let children = match header.base_type {
            1 => {
                self.coordinates(1, header.dimensions)?;
                0
            }
            2 | 8 => {
                let points = u64::from(self.u32(header.little_endian)?);
                self.coordinates(points, header.dimensions)?;
                0
            }
            3 | 17 => {
                let rings = u64::from(self.u32(header.little_endian)?);
                self.add_components(rings)?;
                for _ in 0..rings {
                    let points = u64::from(self.u32(header.little_endian)?);
                    self.coordinates(points, header.dimensions)?;
                }
                0
            }
            4..=7 | 9..=12 | 15 | 16 => u64::from(self.u32(header.little_endian)?),
            13 | 14 => return Err(mapping_error("tipo EWKB astratto non serializzabile")),
            _ => return Err(mapping_error("tipo geometry EWKB non supportato")),
        };
        Ok((children, header))
    }
}

/// Ispeziona una EWKB senza ricorsione e senza allocazioni proporzionali ai
/// conteggi dichiarati dal payload.
///
/// # Errors
///
/// Restituisce `DataMapping` per payload malformati e `ResourceLimit` prima di
/// attraversare conteggi o profondità oltre i limiti.
pub fn inspect_ewkb(bytes: &[u8], max_components: u64, max_depth: u64) -> Result<EwkbStats> {
    inspect_ewkb_detailed(bytes, max_components, max_depth).map(|inspection| inspection.stats)
}

/// Ispeziona struttura e metadati della geometria EWKB radice con gli stessi
/// limiti applicati da [`inspect_ewkb`].
///
/// # Errors
///
/// Restituisce `DataMapping` per payload malformati e `ResourceLimit` prima di
/// attraversare conteggi o profondità oltre i limiti.
pub fn inspect_ewkb_detailed(
    bytes: &[u8],
    max_components: u64,
    max_depth: u64,
) -> Result<EwkbInspection> {
    if max_components == 0 || max_depth == 0 {
        return Err(resource_error("limiti EWKB devono essere maggiori di zero"));
    }
    let mut scanner = Scanner {
        bytes,
        offset: 0,
        components: 0,
        max_components,
        max_depth,
        observed_depth: 0,
        has_any_z: false,
        has_any_m: false,
        has_any_embedded_srid: false,
    };
    let mut depth = 1;
    let mut frames = Vec::<Frame>::new();
    let mut root = None;
    loop {
        let (children, header) = scanner.geometry(depth)?;
        if root.is_none() {
            root = Some(EwkbGeometryMetadata {
                base_type: header.base_type,
                has_z: header.has_z,
                has_m: header.has_m,
                srid: header.srid,
            });
        }
        if children > 0 {
            let child_depth = depth
                .checked_add(1)
                .ok_or_else(|| resource_error("overflow profondità EWKB"))?;
            if child_depth > max_depth {
                return Err(resource_error(
                    "geometria EWKB oltre il limite di profondità",
                ));
            }
            frames.push(Frame {
                remaining: children,
                child_depth,
            });
        }
        loop {
            let Some(frame) = frames.last_mut() else {
                if scanner.offset != bytes.len() {
                    return Err(mapping_error("byte residui dopo la geometria EWKB"));
                }
                return Ok(EwkbInspection {
                    stats: EwkbStats {
                        components: scanner.components,
                        max_depth: scanner.observed_depth,
                    },
                    root: root.ok_or_else(|| mapping_error("geometria EWKB assente"))?,
                    has_any_z: scanner.has_any_z,
                    has_any_m: scanner.has_any_m,
                    has_any_embedded_srid: scanner.has_any_embedded_srid,
                });
            };
            if frame.remaining == 0 {
                frames.pop();
                continue;
            }
            frame.remaining -= 1;
            depth = frame.child_depth;
            break;
        }
    }
}

fn mapping_error(message: &str) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Validate,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: None,
        execution_id: None,
        message: message.to_owned(),
    }
}

fn resource_error(message: &str) -> DatabaseError {
    DatabaseError::resource_limit(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point() -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&0_f64.to_le_bytes());
        bytes.extend_from_slice(&1_f64.to_le_bytes());
        bytes
    }

    fn collection(child: &[u8]) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&7_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(child);
        bytes
    }

    fn point_z_srid() -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&(0xa000_0001_u32).to_le_bytes());
        bytes.extend_from_slice(&4_326_u32.to_le_bytes());
        bytes.extend_from_slice(&0_f64.to_le_bytes());
        bytes.extend_from_slice(&1_f64.to_le_bytes());
        bytes.extend_from_slice(&2_f64.to_le_bytes());
        bytes
    }

    #[test]
    fn counts_components_without_recursive_calls() {
        let bytes = collection(&collection(&point()));
        let stats = inspect_ewkb(&bytes, 10, 3).expect("valid collection");
        assert_eq!(stats.components, 4);
        assert_eq!(stats.max_depth, 3);
    }

    #[test]
    fn rejects_depth_and_component_bombs() {
        let bytes = collection(&collection(&point()));
        assert_eq!(
            inspect_ewkb(&bytes, 10, 2).expect_err("depth").category,
            ErrorCategory::ResourceLimit
        );
        assert_eq!(
            inspect_ewkb(&bytes, 3, 3).expect_err("components").category,
            ErrorCategory::ResourceLimit
        );
    }

    #[test]
    fn rejects_truncation_and_trailing_bytes() {
        let mut bytes = point();
        bytes.pop();
        assert_eq!(
            inspect_ewkb(&bytes, 10, 3).expect_err("truncated").category,
            ErrorCategory::DataMapping
        );
        let mut trailing = point();
        trailing.push(0);
        assert!(inspect_ewkb(&trailing, 10, 3).is_err());
    }

    #[test]
    fn reports_root_contract_metadata_from_the_validated_header() {
        let inspection = inspect_ewkb_detailed(&point_z_srid(), 10, 1).expect("valid point");
        assert_eq!(inspection.stats.components, 2);
        assert_eq!(inspection.root.base_type, 1);
        assert_eq!(inspection.root.dimensions_label(), "xyz");
        assert_eq!(inspection.root.geometry_type_name(), Some("Point"));
        assert_eq!(inspection.root.srid, Some(4_326));
    }
}

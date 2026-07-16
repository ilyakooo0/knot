//! Unwind info generation (`.eh_frame`) for compiled Knot programs.
//!
//! `cranelift-object` does not emit unwind tables itself, so without this
//! module the system unwinder cannot walk Cranelift-generated frames and a
//! Rust panic raised inside a runtime function called from generated code
//! aborts the process instead of reaching the runtime's `catch_unwind`
//! recovery sites (HTTP 500 responses, race-loser cancellation).
//!
//! Adapted from `rustc_codegen_cranelift/src/debuginfo/{unwind,emit,object}.rs`
//! (MIT/Apache-2.0). Generated functions carry no landing pads or personality
//! routine — frames are only ever unwound *through*.

use std::collections::HashMap;

use cranelift_codegen::ir::Endianness;
use cranelift_codegen::isa::unwind::UnwindInfo;
use cranelift_codegen::Context;
use cranelift_module::{DataId, FuncId, Module};
use cranelift_object::{ObjectModule, ObjectProduct};
use gimli::write::{Address, CieId, EhFrame, EndianVec, FrameTable, Result as WriteResult, Section, Writer};
use gimli::{RunTimeEndian, SectionId};
use object::write::{Relocation, StandardSection};
use object::{RelocationEncoding, RelocationFlags};

fn address_for_func(func_id: FuncId) -> Address {
    let symbol = func_id.as_u32();
    assert!(symbol & 1 << 31 == 0);
    Address::Symbol { symbol: symbol as usize, addend: 0 }
}

pub(crate) struct UnwindContext {
    endian: RunTimeEndian,
    frame_table: FrameTable,
    cie_id: Option<CieId>,
}

impl UnwindContext {
    pub(crate) fn new(module: &mut ObjectModule, pic_eh_frame: bool) -> Self {
        let endian = match module.isa().endianness() {
            Endianness::Little => RunTimeEndian::Little,
            Endianness::Big => RunTimeEndian::Big,
        };
        let mut frame_table = FrameTable::default();

        let cie_id = if let Some(mut cie) = module.isa().create_systemv_cie() {
            if pic_eh_frame {
                cie.fde_address_encoding =
                    gimli::DwEhPe(gimli::DW_EH_PE_pcrel.0 | gimli::DW_EH_PE_sdata4.0);
            } else {
                cie.fde_address_encoding = gimli::DW_EH_PE_absptr;
            }
            Some(frame_table.add_cie(cie))
        } else {
            None
        };

        UnwindContext { endian, frame_table, cie_id }
    }

    /// Record the unwind info for a just-defined function. Must be called
    /// after `Module::define_function`, while `context` still holds the
    /// compiled code.
    pub(crate) fn add_function(
        &mut self,
        module: &mut ObjectModule,
        func_id: FuncId,
        context: &Context,
    ) {
        let unwind_info = match context
            .compiled_code()
            .expect("add_function called before compilation")
            .create_unwind_info(module.isa())
            .expect("failed to create unwind info")
        {
            Some(ui) => ui,
            None => return,
        };

        match unwind_info {
            UnwindInfo::SystemV(unwind_info) => {
                if let Some(cie_id) = self.cie_id {
                    let fde = unwind_info.to_fde(address_for_func(func_id));
                    self.frame_table.add_fde(cie_id, fde);
                }
            }
            UnwindInfo::WindowsX64(_) | UnwindInfo::WindowsArm64(_) => {
                // Windows does not use .eh_frame; unsupported here.
            }
            _ => {} // unsupported unwind info format
        }
    }

    /// Write the accumulated frame table into the object's `.eh_frame`
    /// section, with relocations pointing at the function symbols.
    pub(crate) fn emit(self, product: &mut ObjectProduct) {
        let mut eh_frame = EhFrame::from(WriterRelocate::new(self.endian));
        self.frame_table.write_eh_frame(&mut eh_frame).unwrap();

        if eh_frame.0.writer.slice().is_empty() {
            return;
        }

        let id = eh_frame.id();
        let section_id = add_debug_section(product, id, eh_frame.0.writer.into_vec());
        let mut section_map = HashMap::new();
        section_map.insert(id, section_id);

        // Mach-O relocations must target the symbol directly; ELF prefers
        // section-relative.
        let use_section_symbol = product.object.format() != object::BinaryFormat::MachO;
        for reloc in &eh_frame.0.relocs {
            add_debug_reloc(product, &section_map, &section_id, reloc, use_section_symbol);
        }
    }
}

type ObjSectionId = (object::write::SectionId, object::write::SymbolId);

fn add_debug_section(
    product: &mut ObjectProduct,
    id: SectionId,
    data: Vec<u8>,
) -> ObjSectionId {
    assert_eq!(id, SectionId::EhFrame, "only .eh_frame emission is supported");
    let section_id = product.object.section_id(StandardSection::EhFrame);
    product.object.section_mut(section_id).set_data(data, 8);
    let symbol_id = product.object.section_symbol(section_id);
    (section_id, symbol_id)
}

fn add_debug_reloc(
    product: &mut ObjectProduct,
    section_map: &HashMap<SectionId, ObjSectionId>,
    from: &ObjSectionId,
    reloc: &DebugReloc,
    use_section_symbol: bool,
) {
    let (symbol, symbol_offset) = match reloc.name {
        DebugRelocName::Section(id) => (section_map.get(&id).unwrap().1, 0),
        DebugRelocName::Symbol(id) => {
            let id: u32 = id.try_into().unwrap();
            let symbol_id = if id & 1 << 31 == 0 {
                product.function_symbol(FuncId::from_u32(id))
            } else {
                product.data_symbol(DataId::from_u32(id & !(1 << 31)))
            };
            if use_section_symbol {
                product
                    .object
                    .symbol_section_and_offset(symbol_id)
                    .unwrap_or((symbol_id, 0))
            } else {
                (symbol_id, 0)
            }
        }
    };
    product
        .object
        .add_relocation(
            from.0,
            Relocation {
                offset: u64::from(reloc.offset),
                symbol,
                flags: RelocationFlags::Generic {
                    kind: reloc.kind,
                    encoding: RelocationEncoding::Generic,
                    size: reloc.size * 8,
                },
                addend: i64::try_from(symbol_offset).unwrap() + reloc.addend,
            },
        )
        .unwrap();
}

#[derive(Clone)]
struct DebugReloc {
    offset: u32,
    size: u8,
    name: DebugRelocName,
    addend: i64,
    kind: object::RelocationKind,
}

#[derive(Clone)]
enum DebugRelocName {
    Section(SectionId),
    Symbol(usize),
}

/// A [`Writer`] that collects all necessary relocations.
#[derive(Clone)]
struct WriterRelocate {
    relocs: Vec<DebugReloc>,
    writer: EndianVec<RunTimeEndian>,
}

impl WriterRelocate {
    fn new(endian: RunTimeEndian) -> Self {
        WriterRelocate { relocs: Vec::new(), writer: EndianVec::new(endian) }
    }
}

impl Writer for WriterRelocate {
    type Endian = RunTimeEndian;

    fn endian(&self) -> Self::Endian {
        self.writer.endian()
    }

    fn len(&self) -> usize {
        self.writer.len()
    }

    fn write(&mut self, bytes: &[u8]) -> WriteResult<()> {
        self.writer.write(bytes)
    }

    fn write_at(&mut self, offset: usize, bytes: &[u8]) -> WriteResult<()> {
        self.writer.write_at(offset, bytes)
    }

    fn write_address(&mut self, address: Address, size: u8) -> WriteResult<()> {
        match address {
            Address::Constant(val) => self.write_udata(val, size),
            Address::Symbol { symbol, addend } => {
                let offset = self.len() as u64;
                self.relocs.push(DebugReloc {
                    offset: offset as u32,
                    size,
                    name: DebugRelocName::Symbol(symbol),
                    addend,
                    kind: object::RelocationKind::Absolute,
                });
                self.write_udata(0, size)
            }
        }
    }

    fn write_offset(&mut self, val: usize, section: SectionId, size: u8) -> WriteResult<()> {
        let offset = self.len() as u32;
        self.relocs.push(DebugReloc {
            offset,
            size,
            name: DebugRelocName::Section(section),
            addend: val as i64,
            kind: object::RelocationKind::Absolute,
        });
        self.write_udata(0, size)
    }

    fn write_offset_at(
        &mut self,
        offset: usize,
        val: usize,
        section: SectionId,
        size: u8,
    ) -> WriteResult<()> {
        self.relocs.push(DebugReloc {
            offset: offset as u32,
            size,
            name: DebugRelocName::Section(section),
            addend: val as i64,
            kind: object::RelocationKind::Absolute,
        });
        self.write_udata_at(offset, 0, size)
    }

    fn write_eh_pointer(&mut self, address: Address, eh_pe: gimli::DwEhPe, size: u8) -> WriteResult<()> {
        match address {
            // Address::Constant arm copied from gimli
            Address::Constant(val) => {
                // Indirect doesn't matter here.
                let val = match eh_pe.application() {
                    gimli::DW_EH_PE_absptr => val,
                    gimli::DW_EH_PE_pcrel => {
                        let offset = self.len() as u64;
                        offset.wrapping_sub(val)
                    }
                    _ => {
                        return Err(gimli::write::Error::UnsupportedPointerEncoding(eh_pe));
                    }
                };
                self.write_eh_pointer_data(val, eh_pe.format(), size)
            }
            Address::Symbol { symbol, addend } => match eh_pe.application() {
                gimli::DW_EH_PE_pcrel => {
                    let size = match eh_pe.format() {
                        gimli::DW_EH_PE_sdata4 => 4,
                        gimli::DW_EH_PE_sdata8 => 8,
                        _ => return Err(gimli::write::Error::UnsupportedPointerEncoding(eh_pe)),
                    };
                    self.relocs.push(DebugReloc {
                        offset: self.len() as u32,
                        size,
                        name: DebugRelocName::Symbol(symbol),
                        addend,
                        kind: object::RelocationKind::Relative,
                    });
                    self.write_udata(0, size)
                }
                gimli::DW_EH_PE_absptr => {
                    self.relocs.push(DebugReloc {
                        offset: self.len() as u32,
                        size,
                        name: DebugRelocName::Symbol(symbol),
                        addend,
                        kind: object::RelocationKind::Absolute,
                    });
                    self.write_udata(0, size)
                }
                _ => Err(gimli::write::Error::UnsupportedPointerEncoding(eh_pe)),
            },
        }
    }
}

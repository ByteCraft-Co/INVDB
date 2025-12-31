//! Decoding and validation for B-Tree nodes stored in page payloads.

use crate::config::PAGE_SIZE;
use crate::error::{InvError, InvResult};
use crate::page::Page;
use crate::types::PageId;

const PAYLOAD_BASE: usize = 16;

/// Node type discriminator.
#[derive(Clone, Debug)]
pub enum NodeKind {
    Leaf,
    Internal,
}

/// Decoded leaf node representation.
#[derive(Clone, Debug)]
pub struct LeafNode {
    pub num_keys: u16,
    pub next_leaf: PageId,
    pub keys: Vec<u32>,
    pub values: Vec<u64>,
}

/// Decoded internal node representation.
#[derive(Clone, Debug)]
pub struct InternalNode {
    pub num_keys: u16,
    pub children: Vec<PageId>,
    pub keys: Vec<u32>,
}

/// General node wrapper.
#[derive(Clone, Debug)]
pub enum Node {
    Leaf(LeafNode),
    Internal(InternalNode),
}

/// Maximum keys for leaf nodes based on page capacity.
pub fn max_leaf_keys() -> usize {
    // capacity after payload base
    let capacity = PAGE_SIZE - PAYLOAD_BASE;
    // leaf uses 16 bytes header + 12 bytes per key
    (capacity.saturating_sub(16)) / 12
}

/// Maximum keys for internal nodes based on page capacity.
pub fn max_internal_keys() -> usize {
    let capacity = PAGE_SIZE - PAYLOAD_BASE;
    // internal uses 16 bytes header + 8*K + 4 bytes
    (capacity.saturating_sub(20)) / 8
}

/// Construct an empty leaf node.
pub fn empty_leaf() -> Node {
    Node::Leaf(LeafNode {
        num_keys: 0,
        next_leaf: PageId(0),
        keys: Vec::new(),
        values: Vec::new(),
    })
}

/// Encode a node back into a page payload, ensuring invariants.
pub fn encode_into_page(node: &Node, page: &mut crate::page::Page) -> InvResult<()> {
    // Ensure page kind is btree (2)
    if page.as_bytes().get(0).copied() != Some(2) {
        return Err(InvError::Corruption {
            context: "btree.page_kind",
            details: "page header not marked as btree".to_string(),
        });
    }

    let buf = page.as_bytes_mut();
    // zero payload
    for b in &mut buf[PAYLOAD_BASE..] {
        *b = 0;
    }

    match node {
        Node::Leaf(leaf) => encode_leaf(leaf, buf),
        Node::Internal(internal) => encode_internal(internal, buf),
    }
}

fn encode_leaf(leaf: &LeafNode, buf: &mut [u8]) -> InvResult<()> {
    let k = leaf.num_keys as usize;
    if k != leaf.keys.len() || k != leaf.values.len() {
        return Err(InvError::Corruption {
            context: "btree.encode.leaf.size",
            details: "num_keys mismatch with arrays".to_string(),
        });
    }
    if k > max_leaf_keys() {
        return Err(InvError::Corruption {
            context: "btree.encode.leaf.size",
            details: format!("num_keys {} exceeds capacity", k),
        });
    }
    validate_sorted_unique(&leaf.keys, "btree.leaf.keys_order")?;

    buf[PAYLOAD_BASE] = 1; // node_kind leaf
    buf[PAYLOAD_BASE + 1] = 0; // node_flags
    buf[PAYLOAD_BASE + 2..PAYLOAD_BASE + 4].copy_from_slice(&(leaf.num_keys).to_le_bytes());
    buf[PAYLOAD_BASE + 4..PAYLOAD_BASE + 8].copy_from_slice(&0u32.to_le_bytes());
    buf[PAYLOAD_BASE + 8..PAYLOAD_BASE + 12].copy_from_slice(&leaf.next_leaf.0.to_le_bytes());
    buf[PAYLOAD_BASE + 12..PAYLOAD_BASE + 16].copy_from_slice(&0u32.to_le_bytes());

    let keys_offset = PAYLOAD_BASE + 16;
    let values_offset = keys_offset + 4 * k;
    for (i, key) in leaf.keys.iter().enumerate() {
        let offset = keys_offset + 4 * i;
        buf[offset..offset + 4].copy_from_slice(&key.to_le_bytes());
    }
    for (i, value) in leaf.values.iter().enumerate() {
        let offset = values_offset + 8 * i;
        buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn encode_internal(internal: &InternalNode, buf: &mut [u8]) -> InvResult<()> {
    let k = internal.num_keys as usize;
    if k != internal.keys.len() || internal.children.len() != k + 1 {
        return Err(InvError::Corruption {
            context: "btree.encode.internal.size",
            details: "num_keys/children mismatch".to_string(),
        });
    }
    if k > max_internal_keys() {
        return Err(InvError::Corruption {
            context: "btree.encode.internal.size",
            details: format!("num_keys {} exceeds capacity", k),
        });
    }
    validate_sorted_unique(&internal.keys, "btree.internal.keys_order")?;

    buf[PAYLOAD_BASE] = 2; // node_kind internal
    buf[PAYLOAD_BASE + 1] = 0;
    buf[PAYLOAD_BASE + 2..PAYLOAD_BASE + 4].copy_from_slice(&(internal.num_keys).to_le_bytes());
    buf[PAYLOAD_BASE + 4..PAYLOAD_BASE + 8].copy_from_slice(&0u32.to_le_bytes());
    buf[PAYLOAD_BASE + 8..PAYLOAD_BASE + 12].copy_from_slice(&0u32.to_le_bytes());
    buf[PAYLOAD_BASE + 12..PAYLOAD_BASE + 16].copy_from_slice(&0u32.to_le_bytes());

    let children_offset = PAYLOAD_BASE + 16;
    for (i, child) in internal.children.iter().enumerate() {
        let offset = children_offset + 4 * i;
        buf[offset..offset + 4].copy_from_slice(&child.0.to_le_bytes());
    }

    let keys_offset = children_offset + 4 * (k + 1);
    for (i, key) in internal.keys.iter().enumerate() {
        let offset = keys_offset + 4 * i;
        buf[offset..offset + 4].copy_from_slice(&key.to_le_bytes());
    }
    Ok(())
}
impl Node {
    /// Decode and validate a B-Tree node from the page payload.
    pub fn decode(page: &Page, page_count: u32) -> InvResult<Self> {
        let buf = page.as_bytes();
        if buf.len() < PAYLOAD_BASE + 16 {
            return Err(InvError::Corruption {
                context: "btree.leaf.size",
                details: "payload too small".to_string(),
            });
        }

        let node_kind_byte = read_u8(buf, PAYLOAD_BASE, "btree.leaf.size")?;
        let node_flags = read_u8(buf, PAYLOAD_BASE + 1, "btree.leaf.size")?;
        if node_flags != 0 {
            return Err(InvError::Unsupported {
                feature: "btree.node_flags",
            });
        }

        let num_keys = read_u16(buf, PAYLOAD_BASE + 2, "btree.leaf.size")?;
        let reserved = read_u32(buf, PAYLOAD_BASE + 4, "btree.leaf.size")?;
        if reserved != 0 {
            return Err(InvError::Unsupported {
                feature: "btree.reserved",
            });
        }

        match node_kind_byte {
            1 => decode_leaf(buf, num_keys, page_count),
            2 => decode_internal(buf, num_keys, page_count),
            _ => Err(InvError::Corruption {
                context: "btree.node_kind",
                details: format!("unknown kind {}", node_kind_byte),
            }),
        }
    }
}

fn decode_leaf(buf: &[u8], num_keys: u16, page_count: u32) -> InvResult<Node> {
    let k = num_keys as usize;
    let keys_offset = PAYLOAD_BASE + 16;
    let values_offset = keys_offset
        .checked_add(4 * k)
        .ok_or(InvError::Corruption {
            context: "btree.leaf.size",
            details: "keys offset overflow".to_string(),
        })?;
    let end_offset = values_offset
        .checked_add(8 * k)
        .ok_or(InvError::Corruption {
            context: "btree.leaf.size",
            details: "values offset overflow".to_string(),
        })?;

    if end_offset > PAGE_SIZE {
        return Err(InvError::Corruption {
            context: "btree.leaf.size",
            details: format!("num_keys={} exceeds page capacity", num_keys),
        });
    }

    let next_leaf_raw = read_u32(buf, PAYLOAD_BASE + 8, "btree.leaf.size")?;
    let reserved2 = read_u32(buf, PAYLOAD_BASE + 12, "btree.leaf.size")?;
    if reserved2 != 0 {
        return Err(InvError::Corruption {
            context: "btree.leaf.reserved2",
            details: format!("expected 0 got {}", reserved2),
        });
    }

    if next_leaf_raw != 0 && next_leaf_raw >= page_count {
        return Err(InvError::Corruption {
            context: "btree.leaf.next_leaf",
            details: format!(
                "next_leaf {} out of bounds for page_count {}",
                next_leaf_raw, page_count
            ),
        });
    }

    let mut keys = Vec::with_capacity(k);
    for i in 0..k {
        let offset = keys_offset + 4 * i;
        keys.push(read_u32(buf, offset, "btree.leaf.size")?);
    }

    validate_sorted_unique(&keys, "btree.leaf.keys_order")?;

    let mut values = Vec::with_capacity(k);
    for i in 0..k {
        let offset = values_offset + 8 * i;
        values.push(read_u64(buf, offset, "btree.leaf.size")?);
    }

    Ok(Node::Leaf(LeafNode {
        num_keys,
        next_leaf: PageId(next_leaf_raw),
        keys,
        values,
    }))
}

fn decode_internal(buf: &[u8], num_keys: u16, page_count: u32) -> InvResult<Node> {
    let k = num_keys as usize;
    let children_offset = PAYLOAD_BASE + 16;
    let keys_offset = children_offset
        .checked_add(4 * (k + 1))
        .ok_or(InvError::Corruption {
            context: "btree.internal.size",
            details: "children offset overflow".to_string(),
        })?;
    let end_offset = keys_offset
        .checked_add(4 * k)
        .ok_or(InvError::Corruption {
            context: "btree.internal.size",
            details: "keys offset overflow".to_string(),
        })?;

    if end_offset > PAGE_SIZE {
        return Err(InvError::Corruption {
            context: "btree.internal.size",
            details: format!("num_keys={} exceeds page capacity", num_keys),
        });
    }

    let reserved2 = read_u32(buf, PAYLOAD_BASE + 8, "btree.internal.size")?;
    if reserved2 != 0 {
        return Err(InvError::Corruption {
            context: "btree.internal.reserved2",
            details: format!("expected 0 got {}", reserved2),
        });
    }
    let reserved3 = read_u32(buf, PAYLOAD_BASE + 12, "btree.internal.size")?;
    if reserved3 != 0 {
        return Err(InvError::Corruption {
            context: "btree.internal.reserved3",
            details: format!("expected 0 got {}", reserved3),
        });
    }

    let mut children = Vec::with_capacity(k + 1);
    for i in 0..(k + 1) {
        let offset = children_offset + 4 * i;
        let child = read_u32(buf, offset, "btree.internal.size")?;
        if child == 0 || child >= page_count {
            return Err(InvError::Corruption {
                context: "btree.internal.child",
                details: format!(
                    "child {} out of bounds for page_count {}",
                    child, page_count
                ),
            });
        }
        children.push(PageId(child));
    }

    let mut keys = Vec::with_capacity(k);
    for i in 0..k {
        let offset = keys_offset + 4 * i;
        keys.push(read_u32(buf, offset, "btree.internal.size")?);
    }

    validate_sorted_unique(&keys, "btree.internal.keys_order")?;

    Ok(Node::Internal(InternalNode {
        num_keys,
        children,
        keys,
    }))
}

fn validate_sorted_unique(keys: &[u32], context: &'static str) -> InvResult<()> {
    for window in keys.windows(2) {
        if let [a, b] = window {
            if a >= b {
                return Err(InvError::Corruption {
                    context,
                    details: format!("keys not strictly increasing: {} >= {}", a, b),
                });
            }
        }
    }
    Ok(())
}

fn read_u8(buf: &[u8], offset: usize, context: &'static str) -> InvResult<u8> {
    buf.get(offset)
        .copied()
        .ok_or(InvError::Corruption {
            context,
            details: "unexpected end of buffer".to_string(),
        })
}

fn read_u16(buf: &[u8], offset: usize, context: &'static str) -> InvResult<u16> {
    let slice = buf
        .get(offset..offset + 2)
        .ok_or(InvError::Corruption {
            context,
            details: "unexpected end of buffer".to_string(),
        })?;
    Ok(u16::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u32(buf: &[u8], offset: usize, context: &'static str) -> InvResult<u32> {
    let slice = buf
        .get(offset..offset + 4)
        .ok_or(InvError::Corruption {
            context,
            details: "unexpected end of buffer".to_string(),
        })?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u64(buf: &[u8], offset: usize, context: &'static str) -> InvResult<u64> {
    let slice = buf
        .get(offset..offset + 8)
        .ok_or(InvError::Corruption {
            context,
            details: "unexpected end of buffer".to_string(),
        })?;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

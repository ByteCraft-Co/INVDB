use crate::btree::node::Node;
use crate::error::{InvError, InvResult};
use crate::pager::Pager;
use crate::types::PageId;

const MAX_DEPTH: usize = 64;

/// Read-only search for a u32 key, returning the associated u64 value if present.
pub fn search_u64(pager: &mut Pager, root: PageId, key: u32) -> InvResult<Option<u64>> {
    let mut current = root;
    let mut depth = 0usize;

    loop {
        if depth > MAX_DEPTH {
            return Err(InvError::Corruption {
                context: "btree.depth",
                details: format!("exceeded depth {}", MAX_DEPTH),
            });
        }

        if current.0 == 0 {
            return Err(InvError::Corruption {
                context: "btree.traverse.header",
                details: "encountered header page".to_string(),
            });
        }

        let page_count = pager.page_count();
        let page = pager.get_page(current)?;
        let page_buf = page.as_bytes();
        let page_kind = page_buf
            .get(0)
            .copied()
            .ok_or(InvError::Corruption {
                context: "btree.page_kind",
                details: "missing page header".to_string(),
            })?;
        if page_kind != 2 {
            return Err(InvError::Corruption {
                context: "btree.page_kind",
                details: format!("expected 2 got {}", page_kind),
            });
        }

        let node = Node::decode(page, page_count)?;
        match node {
            Node::Leaf(leaf) => match leaf.keys.binary_search(&key) {
                Ok(idx) => return Ok(Some(leaf.values[idx])),
                Err(_) => return Ok(None),
            },
            Node::Internal(internal) => {
                let idx = internal
                    .keys
                    .iter()
                    .position(|&k| key < k)
                    .unwrap_or(internal.keys.len());
                current = internal.children[idx];
                depth += 1;
            }
        }
    }
}

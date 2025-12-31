use crate::btree::node::{encode_into_page, max_internal_keys, max_leaf_keys, Node};
use crate::btree::split::{split_internal, split_leaf};
use crate::error::InvResult;
use crate::pager::Pager;
use crate::types::PageId;

enum InsertResult {
    NoSplit,
    Split { promoted_key: u32, right: PageId },
}

pub fn insert_u64(
    pager: &mut Pager,
    root: PageId,
    key: u32,
    value: u64,
) -> InvResult<PageId> {
    let result = insert_into(pager, root, key, value)?;
    match result {
        InsertResult::NoSplit => Ok(root),
        InsertResult::Split {
            promoted_key,
            right,
        } => {
            // Need a new root
            let new_root_id = pager.allocate_btree_page()?;
            let mut children = vec![root, right];
            let mut keys = vec![promoted_key];
            let internal = Node::Internal(crate::btree::node::InternalNode {
                num_keys: keys.len() as u16,
                children: children.drain(..).collect(),
                keys: keys.drain(..).collect(),
            });
            encode_into_page(&internal, pager.get_page_mut(new_root_id)?)?;
            Ok(new_root_id)
        }
    }
}

fn insert_into(pager: &mut Pager, page_id: PageId, key: u32, value: u64) -> InvResult<InsertResult> {
    let page_count = pager.page_count();
    let page = pager.get_page(page_id)?;
    let mut node = Node::decode(page, page_count)?;

    match &mut node {
        Node::Leaf(leaf) => {
            match leaf.keys.binary_search(&key) {
                Ok(idx) => {
                    leaf.values[idx] = value;
                    encode_into_page(&node, pager.get_page_mut(page_id)?)?;
                    return Ok(InsertResult::NoSplit);
                }
                Err(pos) => {
                    leaf.keys.insert(pos, key);
                    leaf.values.insert(pos, value);
                    leaf.num_keys += 1;
                    if (leaf.num_keys as usize) <= max_leaf_keys() {
                        encode_into_page(&node, pager.get_page_mut(page_id)?)?;
                        Ok(InsertResult::NoSplit)
                    } else {
                        let Node::Leaf(leaf_node) = node else {
                            unreachable!()
                        };
                        let split = split_leaf(pager, page_id, leaf_node)?;
                        Ok(InsertResult::Split {
                            promoted_key: split.promoted_key,
                            right: split.right_page,
                        })
                    }
                }
            }
        }
        Node::Internal(internal) => {
            let idx = internal
                .keys
                .iter()
                .position(|&k| key < k)
                .unwrap_or(internal.keys.len());
            let child_id = internal.children[idx];
            let child_result = insert_into(pager, child_id, key, value)?;
            match child_result {
                InsertResult::NoSplit => Ok(InsertResult::NoSplit),
                InsertResult::Split {
                    promoted_key,
                    right,
                } => {
                    internal.keys.insert(idx, promoted_key);
                    internal.children.insert(idx + 1, right);
                    internal.num_keys += 1;
                    if (internal.num_keys as usize) <= max_internal_keys() {
                        encode_into_page(&node, pager.get_page_mut(page_id)?)?;
                        Ok(InsertResult::NoSplit)
                    } else {
                        let Node::Internal(int_node) = node else {
                            unreachable!()
                        };
                        let split = split_internal(pager, page_id, int_node)?;
                        Ok(InsertResult::Split {
                            promoted_key: split.promoted_key,
                            right: split.right_page,
                        })
                    }
                }
            }
        }
    }
}

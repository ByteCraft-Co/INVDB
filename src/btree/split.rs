use crate::btree::node::{max_internal_keys, max_leaf_keys, InternalNode, LeafNode};
use crate::error::InvResult;
use crate::pager::Pager;
use crate::types::PageId;

pub struct SplitResult {
    pub promoted_key: u32,
    pub right_page: PageId,
}

pub fn split_leaf(
    pager: &mut Pager,
    page_id: PageId,
    mut node: LeafNode,
) -> InvResult<SplitResult> {
    let total_keys = node.num_keys as usize;
    debug_assert!(total_keys > max_leaf_keys());

    let mid = total_keys / 2;

    let mut right_keys = node.keys.split_off(mid);
    let mut right_values = node.values.split_off(mid);

    let promoted_key = right_keys[0];

    let right_next = node.next_leaf;
    let right_page_id = pager.allocate_btree_page()?;

    let left_next = right_page_id;
    node.next_leaf = left_next;

    let right_node = LeafNode {
        num_keys: right_keys.len() as u16,
        next_leaf: right_next,
        keys: right_keys.drain(..).collect(),
        values: right_values.drain(..).collect(),
    };

    node.num_keys = node.keys.len() as u16;

    pager.encode_leaf_into_page(page_id, &node)?;
    pager.encode_leaf_into_page(right_page_id, &right_node)?;

    Ok(SplitResult {
        promoted_key,
        right_page: right_page_id,
    })
}

pub fn split_internal(
    pager: &mut Pager,
    page_id: PageId,
    mut node: InternalNode,
) -> InvResult<SplitResult> {
    let total_keys = node.num_keys as usize;
    debug_assert!(total_keys > max_internal_keys());

    let mid = total_keys / 2;
    let promoted_key = node.keys[mid];

    let right_keys: Vec<u32> = node.keys.split_off(mid + 1);
    let right_children: Vec<PageId> = node.children.split_off(mid + 1);

    let left_keys = node.keys.clone();
    let left_children = node.children.clone();

    let right_node = InternalNode {
        num_keys: right_keys.len() as u16,
        children: right_children,
        keys: right_keys,
    };

    let left_node = InternalNode {
        num_keys: left_keys.len() as u16,
        children: left_children,
        keys: left_keys,
    };

    let right_page_id = pager.allocate_btree_page()?;

    pager.encode_internal_into_page(page_id, &left_node)?;
    pager.encode_internal_into_page(right_page_id, &right_node)?;

    Ok(SplitResult {
        promoted_key,
        right_page: right_page_id,
    })
}

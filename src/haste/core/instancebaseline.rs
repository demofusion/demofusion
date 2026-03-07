use core::num::ParseIntError;
use std::sync::Arc;

use sync_unsafe_cell::SyncUnsafeCell;

use super::stringtables::StringTable;

pub(crate) const INSTANCE_BASELINE_TABLE_NAME: &str = "instancebaseline";

#[derive(Default)]
pub(crate) struct InstanceBaseline {
    data: Vec<Option<Arc<SyncUnsafeCell<Vec<u8>>>>>,
}

impl InstanceBaseline {
    pub(crate) fn update(
        &mut self,
        string_table: &StringTable,
        classes: usize,
    ) -> Result<(), ParseIntError> {
        if self.data.len() < classes {
            self.data.resize(classes, None);
        }

        for (_entity_index, item) in string_table.items() {
            let data = item.string.as_ref();
            if let Some(data) = data
                && let Ok(string) = core::str::from_utf8(data)
            {
                let class_id = string.parse::<i32>()?;
                self.data[class_id as usize].clone_from(&item.user_data);
            }
        }
        Ok(())
    }

    #[allow(unsafe_code)]
    #[inline]
    pub(crate) unsafe fn by_id_unchecked(&self, class_id: i32) -> &[u8] {
        unsafe {
            &*self
                .data
                .get_unchecked(class_id as usize)
                .as_ref()
                .unwrap_unchecked()
                .get()
        }
    }

    /// clear clears underlying storage, but this has no effect on the allocated capacity.
    pub(crate) fn clear(&mut self) {
        self.data.clear();
    }
}

use std::{path::Path, sync::Arc};

use crate::{
    Result,
    sstable::{Sstable, TableCursor},
};

pub(crate) const INPUT_COUNT: usize = 4;

pub(crate) fn merge(directory: &Path, id: u64, inputs: &[Arc<Sstable>]) -> Result<Option<Sstable>> {
    Sstable::build(directory, id, |output| {
        let mut cursors = inputs
            .iter()
            .map(|table| table.cursor())
            .collect::<Result<Vec<_>>>()?;
        while let Some(winner) = next_winner(&cursors) {
            let (older, suffix) = cursors.split_at_mut(winner);
            let (winning, newer) = suffix.split_first_mut().expect("winner is a cursor");
            let (key, value) = winning.current().expect("winner has a record");
            // The inputs start with the oldest table. A deleted key has no
            // older version outside the merge and needs no output record.
            if let Some(value) = value {
                output.push(key, Some(value))?;
            }
            for cursor in older.iter_mut().chain(newer) {
                if cursor
                    .current()
                    .is_some_and(|(candidate, _)| candidate == key)
                {
                    cursor.advance()?;
                }
            }
            winning.advance()?;
        }
        Ok(())
    })
}

fn next_winner(cursors: &[TableCursor<'_>]) -> Option<usize> {
    let mut winner = None;
    for (position, cursor) in cursors.iter().enumerate() {
        if let Some((key, _)) = cursor.current()
            && winner.is_none_or(|(_, smallest)| key <= smallest)
        {
            // Later inputs are newer and win ties.
            winner = Some((position, key));
        }
    }
    winner.map(|(position, _)| position)
}

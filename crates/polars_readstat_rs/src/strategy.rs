use std::cmp;

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionStrategy {
    pub row_partitions: usize,    // Number of row splits
    pub col_partitions: usize,    // Number of column splits per row partition
    pub total_readers: usize,     // row_partitions * col_partitions
}

pub fn calculate_partition_strategy(
    chunk_size: usize,
    total_rows: usize,
    total_cols: usize,
    max_threads: usize,
) -> PartitionStrategy {
    if chunk_size == 0 || total_rows == 0 || total_cols == 0 || max_threads == 0 {
        panic!("All parameters must be greater than 0");
    }

    // Calculate maximum useful partitions based on work available
    let num_chunks = (total_rows + chunk_size - 1) / chunk_size; // Ceiling division
    let max_useful_partitions = num_chunks * total_cols;
    
    // Don't use more threads than we have useful work
    let effective_max_threads = std::cmp::min(max_threads, max_useful_partitions);
    
    let (row_partitions, col_partitions) = find_optimal_partitions(total_cols, effective_max_threads, num_chunks);
    
    PartitionStrategy {
        row_partitions,
        col_partitions,
        total_readers: row_partitions * col_partitions,
    }
}

fn find_optimal_partitions(total_cols: usize, max_threads: usize, num_chunks: usize) -> (usize, usize) {
    // Maximum row partitions is limited by number of chunks available
    let max_row_partitions = std::cmp::min(max_threads, num_chunks);
    
    // If we have >= 2x columns as threads, use pure column partitioning
    if total_cols >= 2 * max_threads {
        return (1, std::cmp::min(max_threads, total_cols));
    }
    
    // Special case: if we have only 1 column, partition by rows/chunks
    if total_cols == 1 {
        return (std::cmp::min(max_threads, num_chunks), 1);
    }
    
    // Otherwise, partition by rows until we have 2x as many columns as threads per row partition
    for row_partitions in 1..=max_row_partitions {
        let threads_per_row_partition = max_threads / row_partitions;
        if threads_per_row_partition == 0 {
            break;
        }
        
        // Check if we now have 2x columns per thread in this row partition
        if total_cols >= 2 * threads_per_row_partition {
            let col_partitions = std::cmp::min(threads_per_row_partition, total_cols);
            return (row_partitions, col_partitions);
        }
    }
    
    // Fallback: distribute available threads optimally without the 2x constraint
    let row_partitions = std::cmp::min(max_row_partitions, total_cols);
    let col_partitions = std::cmp::min(
        std::cmp::max(1, max_threads / row_partitions),
        total_cols
    );
    (row_partitions, col_partitions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pure_column_partitioning() {
        // 20 columns, 4 threads -> 20 >= 2*4, so pure column partitioning
        let strategy = calculate_partition_strategy(100, 1000, 20, 4);
        
        assert_eq!(strategy.row_partitions, 1);
        assert_eq!(strategy.col_partitions, 4);
        assert_eq!(strategy.total_readers, 4);
    }

    #[test]
    fn test_hybrid_partitioning() {
        // 8 columns, 8 threads -> need to split by rows first
        // 8 < 2*8, so split into 2 row partitions of 4 threads each
        // Now 8 >= 2*4, so can use 4 column partitions per row partition
        let strategy = calculate_partition_strategy(100, 1000, 8, 8);
        
        assert_eq!(strategy.row_partitions, 2);
        assert_eq!(strategy.col_partitions, 4);
        assert_eq!(strategy.total_readers, 8);
    }

    #[test]
    fn test_limited_by_chunks() {
        // 50 rows, chunk_size 100 -> only 1 chunk available
        // Even with 2 columns and 8 threads, can only use 2 partitions max
        let strategy = calculate_partition_strategy(100, 50, 2, 8);
        
        assert_eq!(strategy.total_readers, 2); // Limited by 1 chunk * 2 columns
    }

    #[test]
    fn test_very_small_data() {
        // 50 rows, chunk_size 100, 1 column -> only 1 partition possible
        let strategy = calculate_partition_strategy(100, 50, 1, 8);
        
        assert_eq!(strategy.row_partitions, 1);
        assert_eq!(strategy.col_partitions, 1);
        assert_eq!(strategy.total_readers, 1);
    }

    #[test]
    fn test_single_column_many_chunks() {
        // 1 column, 10 chunks, 10 threads -> should partition by rows/chunks
        let strategy = calculate_partition_strategy(100_000, 1_000_000, 1, 10);
        
        assert_eq!(strategy.row_partitions, 10); // One partition per chunk
        assert_eq!(strategy.col_partitions, 1);  // Only 1 column
        assert_eq!(strategy.total_readers, 10);
    }

    #[test]
    fn test_single_column_limited_threads() {
        // 1 column, 10 chunks, 5 threads -> should use 5 row partitions
        let strategy = calculate_partition_strategy(100_000, 1_000_000, 1, 5);
        
        assert_eq!(strategy.row_partitions, 5);
        assert_eq!(strategy.col_partitions, 1);
        assert_eq!(strategy.total_readers, 5);
    }
}
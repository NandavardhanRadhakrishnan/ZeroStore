# ZeroStore

Key Value database made from scratch in Go  
<br>  
Work In Progress. Not production ready just made for learning.

## Current Efficiency 


| Field Type           | Size (bytes) |
|----------------------|--------------|
| Text Field Size      | 1024         |
| Numeric Field Size   | 8            |
| UUID Size            | 36           |
| **Total Row Size**   | **1068**     |
| **Expected Total File Size for 1,000,000 rows** | **1,068,000,000** bytes |
| **Actual Data File Size** | **1,230,961,896** bytes |

### ZeroStore Stats

- **Index File Size**: 106 MB
- **Efficiency Percentage**: 86.76%

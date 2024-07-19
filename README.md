# ZeroStore

Key Value database made from scratch in Go
<br>
Work In Progress. Not production ready just made for learning

## Current Stats

### Ideal Size Calculation

| Field Type           | Size (bytes) |
|----------------------|--------------|
| Text Field Size      | 1024         |
| Numeric Field Size   | 8            |
| Timestamp Size       | -1           |
| UUID Size            | 36           |
| **Total Row Size**   | **1067**     |
| **Total File Size for 1,000,000 rows** | **1,067,000,000** bytes |

### ZeroStore Stats

- **Data File Size**: 1,268,962,268 bytes
- **Index File Size**: 106 MB

### **Compression Ratio: 84%**

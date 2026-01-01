# Micro-Benchmarks
Backend uses a [Divan](https://github.com/nvzqz/divan) implementation for micro benching

## Environment
All tests unless specified are ran under these specs:
 - OS: Arch Linux x86_64
 - Kernel: Linux 6.17.7-zen1-1-zen (or newer)
 - CPU: AMD Ryzen 7 5800X3D (8c-16t) @ 4.55 GHz
   - L1: 512 KB
   - L2: 4 MB
   - L3: 96 MB
 - Memory: 64 GB DDR4 @ 3200mhz (CL18-22-22-42)

## f35dd4c (16 Threads)
```
Timer precision: 20 ns
io                                                               fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ fetch                                                                       │               │               │               │         │
│  ├─ extraction                                                               │               │               │               │         │
│  │  ├─ 8                                                       294.4 ns      │ 2.118 µs      │ 1.203 µs      │ 1.289 µs      │ 1024    │ 1048576
│  │  │                                                          3.395 Mitem/s │ 471.9 Kitem/s │ 830.7 Kitem/s │ 775.3 Kitem/s │         │
│  │  ├─ 16                                                      180.2 ns      │ 3.668 µs      │ 2.496 µs      │ 2.289 µs      │ 1024    │ 1048576
│  │  │                                                          5.548 Mitem/s │ 272.5 Kitem/s │ 400.5 Kitem/s │ 436.8 Kitem/s │         │
│  │  ├─ 32                                                      180.5 ns      │ 3.143 µs      │ 2.479 µs      │ 2.255 µs      │ 1024    │ 1048576
│  │  │                                                          5.538 Mitem/s │ 318.1 Kitem/s │ 403.2 Kitem/s │ 443.3 Kitem/s │         │
│  │  ├─ 64                                                      100.7 ns      │ 3.178 µs      │ 2.51 µs       │ 2.259 µs      │ 1024    │ 1048576
│  │  │                                                          9.928 Mitem/s │ 314.6 Kitem/s │ 398.3 Kitem/s │ 442.5 Kitem/s │         │
│  │  ╰─ 128                                                     201.1 ns      │ 3.31 µs       │ 1.886 µs      │ 2.062 µs      │ 1024    │ 1048576
│  │                                                             4.971 Mitem/s │ 302 Kitem/s   │ 530 Kitem/s   │ 484.8 Kitem/s │         │
│  ╰─ insertion_extraction                                                     │               │               │               │         │
│     ├─ 8                                                       519.9 ns      │ 5.691 µs      │ 1.466 µs      │ 1.518 µs      │ 1024    │ 1048576
│     │                                                          1.923 Mitem/s │ 175.6 Kitem/s │ 681.8 Kitem/s │ 658.7 Kitem/s │         │
│     ├─ 16                                                      383.6 ns      │ 5.197 µs      │ 1.491 µs      │ 1.505 µs      │ 1024    │ 1048576
│     │                                                          2.606 Mitem/s │ 192.4 Kitem/s │ 670.4 Kitem/s │ 664.2 Kitem/s │         │
│     ├─ 32                                                      457.3 ns      │ 4.463 µs      │ 1.451 µs      │ 1.466 µs      │ 1024    │ 1048576
│     │                                                          2.186 Mitem/s │ 224 Kitem/s   │ 688.8 Kitem/s │ 682 Kitem/s   │         │
│     ├─ 64                                                      565.9 ns      │ 2.646 µs      │ 1.437 µs      │ 1.436 µs      │ 1024    │ 1048576
│     │                                                          1.766 Mitem/s │ 377.8 Kitem/s │ 695.8 Kitem/s │ 696.3 Kitem/s │         │
│     ╰─ 128                                                     678.9 ns      │ 2.164 µs      │ 1.41 µs       │ 1.381 µs      │ 1024    │ 1048576
│                                                                1.472 Mitem/s │ 462 Kitem/s   │ 708.8 Kitem/s │ 724 Kitem/s   │         │
╰─ push                                                                        │               │               │               │         │
   ├─ colliding                                                                │               │               │               │         │
   │  ├─ DatabaseConfig { shards: 8, optimistic_read: false }    153 ns        │ 2.612 µs      │ 1.747 µs      │ 1.714 µs      │ 1024    │ 1048576
   │  │                                                          6.533 Mitem/s │ 382.7 Kitem/s │ 572.3 Kitem/s │ 583.2 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 8, optimistic_read: true }     36.14 ns      │ 1.474 µs      │ 1.393 µs      │ 1.363 µs      │ 1024    │ 1048576
   │  │                                                          27.66 Mitem/s │ 678 Kitem/s   │ 717.8 Kitem/s │ 733.2 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 16, optimistic_read: false }   435.8 ns      │ 2.315 µs      │ 1.649 µs      │ 1.618 µs      │ 1024    │ 1048576
   │  │                                                          2.294 Mitem/s │ 431.9 Kitem/s │ 606.2 Kitem/s │ 618 Kitem/s   │         │
   │  ├─ DatabaseConfig { shards: 16, optimistic_read: true }    27.55 ns      │ 1.455 µs      │ 1.387 µs      │ 1.364 µs      │ 1024    │ 1048576
   │  │                                                          36.28 Mitem/s │ 687 Kitem/s   │ 720.7 Kitem/s │ 732.9 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 32, optimistic_read: false }   727.8 ns      │ 2.064 µs      │ 1.689 µs      │ 1.652 µs      │ 1024    │ 1048576
   │  │                                                          1.373 Mitem/s │ 484.3 Kitem/s │ 591.7 Kitem/s │ 605 Kitem/s   │         │
   │  ├─ DatabaseConfig { shards: 32, optimistic_read: true }    27.94 ns      │ 1.548 µs      │ 1.391 µs      │ 1.371 µs      │ 1024    │ 1048576
   │  │                                                          35.79 Mitem/s │ 645.6 Kitem/s │ 718.7 Kitem/s │ 729.1 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 64, optimistic_read: false }   269 ns        │ 2.152 µs      │ 1.72 µs       │ 1.684 µs      │ 1024    │ 1048576
   │  │                                                          3.717 Mitem/s │ 464.5 Kitem/s │ 581.3 Kitem/s │ 593.7 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 64, optimistic_read: true }    34.63 ns      │ 1.566 µs      │ 1.373 µs      │ 1.335 µs      │ 1024    │ 1048576
   │  │                                                          28.87 Mitem/s │ 638.3 Kitem/s │ 728 Kitem/s   │ 748.9 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 128, optimistic_read: false }  188.3 ns      │ 2.309 µs      │ 1.704 µs      │ 1.674 µs      │ 1024    │ 1048576
   │  │                                                          5.308 Mitem/s │ 432.9 Kitem/s │ 586.5 Kitem/s │ 597.3 Kitem/s │         │
   │  ╰─ DatabaseConfig { shards: 128, optimistic_read: true }   27.61 ns      │ 1.429 µs      │ 1.367 µs      │ 1.317 µs      │ 1024    │ 1048576
   │                                                             36.2 Mitem/s  │ 699.5 Kitem/s │ 731.1 Kitem/s │ 759.2 Kitem/s │         │
   ╰─ non_colliding                                                            │               │               │               │         │
      ├─ DatabaseConfig { shards: 8, optimistic_read: false }    455.8 ns      │ 5.458 µs      │ 615.4 ns      │ 838.8 ns      │ 1024    │ 1048576
      │                                                          2.193 Mitem/s │ 183.1 Kitem/s │ 1.624 Mitem/s │ 1.192 Mitem/s │         │
      ├─ DatabaseConfig { shards: 8, optimistic_read: true }     720.3 ns      │ 6.04 µs       │ 902.4 ns      │ 1.121 µs      │ 1024    │ 1048576
      │                                                          1.388 Mitem/s │ 165.5 Kitem/s │ 1.108 Mitem/s │ 891.3 Kitem/s │         │
      ├─ DatabaseConfig { shards: 16, optimistic_read: false }   104 ns        │ 4.451 µs      │ 370 ns        │ 567.2 ns      │ 1024    │ 1048576
      │                                                          9.611 Mitem/s │ 224.6 Kitem/s │ 2.702 Mitem/s │ 1.762 Mitem/s │         │
      ├─ DatabaseConfig { shards: 16, optimistic_read: true }    375.2 ns      │ 4.007 µs      │ 488.1 ns      │ 672.3 ns      │ 1024    │ 1048576
      │                                                          2.664 Mitem/s │ 249.5 Kitem/s │ 2.048 Mitem/s │ 1.487 Mitem/s │         │
      ├─ DatabaseConfig { shards: 32, optimistic_read: false }   80.17 ns      │ 2.907 µs      │ 276.1 ns      │ 413.7 ns      │ 1024    │ 1048576
      │                                                          12.47 Mitem/s │ 343.8 Kitem/s │ 3.62 Mitem/s  │ 2.416 Mitem/s │         │
      ├─ DatabaseConfig { shards: 32, optimistic_read: true }    95.39 ns      │ 2.732 µs      │ 328.7 ns      │ 460.4 ns      │ 1024    │ 1048576
      │                                                          10.48 Mitem/s │ 365.9 Kitem/s │ 3.041 Mitem/s │ 2.171 Mitem/s │         │
      ├─ DatabaseConfig { shards: 64, optimistic_read: false }   86.46 ns      │ 2.098 µs      │ 249.1 ns      │ 347.1 ns      │ 1024    │ 1048576
      │                                                          11.56 Mitem/s │ 476.5 Kitem/s │ 4.013 Mitem/s │ 2.88 Mitem/s  │         │
      ├─ DatabaseConfig { shards: 64, optimistic_read: true }    93.75 ns      │ 2.153 µs      │ 272.3 ns      │ 365.4 ns      │ 1024    │ 1048576
      │                                                          10.66 Mitem/s │ 464.4 Kitem/s │ 3.671 Mitem/s │ 2.735 Mitem/s │         │
      ├─ DatabaseConfig { shards: 128, optimistic_read: false }  85.44 ns      │ 1.911 µs      │ 242.1 ns      │ 325.3 ns      │ 1024    │ 1048576
      │                                                          11.7 Mitem/s  │ 523.2 Kitem/s │ 4.129 Mitem/s │ 3.073 Mitem/s │         │
      ╰─ DatabaseConfig { shards: 128, optimistic_read: true }   97.96 ns      │ 1.549 µs      │ 262.1 ns      │ 332.6 ns      │ 1024    │ 1048576
                                                                 10.2 Mitem/s  │ 645.4 Kitem/s │ 3.815 Mitem/s │ 3.006 Mitem/s │         │
```
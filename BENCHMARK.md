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

## Suite (module layout)
- io
  - fetch
    - extraction
      - Performs ExtractDb::fetch_next() as fast as it can, with valid items in the database.
    - insertion_extraction
      - Performs a ExtractDb::push(item) and then attempts to ExtractDb::fetch_next() as fast as it can. 
  - push
    - non_colliding
      - Performs a ExtractDb::push(item) where item is a guaranteed unique occurrence.
    - colliding
      - Performs a ExtractDb::push(item) where item is a constant.

## 358c0fd (16 Threads)
```
Timer precision: 20 ns
io                                                               fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ fetch                                                                       │               │               │               │         │
│  ├─ extraction                                                               │               │               │               │         │
│  │  ├─ 8                                                                     │               │               │               │         │
│  │  │  ├─ t=1                                                  30.4 ns       │ 154.3 ns      │ 34.49 ns      │ 35.65 ns      │ 1024    │ 1048576
│  │  │  │                                                       32.88 Mitem/s │ 6.476 Mitem/s │ 28.99 Mitem/s │ 28.04 Mitem/s │         │
│  │  │  ╰─ t=16                                                 336.9 ns      │ 2.323 µs      │ 1.247 µs      │ 1.318 µs      │ 1024    │ 1048576
│  │  │                                                          2.967 Mitem/s │ 430.2 Kitem/s │ 801.8 Kitem/s │ 758.2 Kitem/s │         │
│  │  ├─ 16                                                                    │               │               │               │         │
│  │  │  ├─ t=1                                                  26.74 ns      │ 45.81 ns      │ 29.33 ns      │ 30.21 ns      │ 1024    │ 1048576
│  │  │  │                                                       37.38 Mitem/s │ 21.82 Mitem/s │ 34.09 Mitem/s │ 33.1 Mitem/s  │         │
│  │  │  ╰─ t=16                                                 178.9 ns      │ 3.217 µs      │ 2.537 µs      │ 2.305 µs      │ 1024    │ 1048576
│  │  │                                                          5.587 Mitem/s │ 310.7 Kitem/s │ 394 Kitem/s   │ 433.6 Kitem/s │         │
│  │  ├─ 32                                                                    │               │               │               │         │
│  │  │  ├─ t=1                                                  27.79 ns      │ 145.5 ns      │ 30.24 ns      │ 31.09 ns      │ 1024    │ 1048576
│  │  │  │                                                       35.97 Mitem/s │ 6.87 Mitem/s  │ 33.06 Mitem/s │ 32.16 Mitem/s │         │
│  │  │  ╰─ t=16                                                 187.2 ns      │ 3.147 µs      │ 2.389 µs      │ 2.179 µs      │ 1024    │ 1048576
│  │  │                                                          5.34 Mitem/s  │ 317.7 Kitem/s │ 418.4 Kitem/s │ 458.7 Kitem/s │         │
│  │  ├─ 64                                                                    │               │               │               │         │
│  │  │  ├─ t=1                                                  28.3 ns       │ 62.43 ns      │ 32.35 ns      │ 32.84 ns      │ 1024    │ 1048576
│  │  │  │                                                       35.33 Mitem/s │ 16.01 Mitem/s │ 30.9 Mitem/s  │ 30.44 Mitem/s │         │
│  │  │  ╰─ t=16                                                 177.5 ns      │ 3.168 µs      │ 2.625 µs      │ 2.382 µs      │ 1024    │ 1048576
│  │  │                                                          5.631 Mitem/s │ 315.6 Kitem/s │ 380.9 Kitem/s │ 419.7 Kitem/s │         │
│  │  ╰─ 128                                                                   │               │               │               │         │
│  │     ├─ t=1                                                  31.34 ns      │ 66.57 ns      │ 37.9 ns       │ 38.24 ns      │ 1024    │ 1048576
│  │     │                                                       31.9 Mitem/s  │ 15.02 Mitem/s │ 26.37 Mitem/s │ 26.15 Mitem/s │         │
│  │     ╰─ t=16                                                 218 ns        │ 3.373 µs      │ 1.81 µs       │ 1.978 µs      │ 1024    │ 1048576
│  │                                                             4.585 Mitem/s │ 296.3 Kitem/s │ 552.4 Kitem/s │ 505.4 Kitem/s │         │
│  ╰─ insertion_extraction                                                     │               │               │               │         │
│     ├─ 8                                                       519.5 ns      │ 5.336 µs      │ 1.462 µs      │ 1.513 µs      │ 1024    │ 1048576
│     │                                                          1.924 Mitem/s │ 187.3 Kitem/s │ 683.7 Kitem/s │ 660.7 Kitem/s │         │
│     ├─ 16                                                      373.8 ns      │ 5.69 µs       │ 1.438 µs      │ 1.469 µs      │ 1024    │ 1048576
│     │                                                          2.674 Mitem/s │ 175.7 Kitem/s │ 695.4 Kitem/s │ 680.6 Kitem/s │         │
│     ├─ 32                                                      319.1 ns      │ 3.684 µs      │ 1.466 µs      │ 1.449 µs      │ 1024    │ 1048576
│     │                                                          3.133 Mitem/s │ 271.4 Kitem/s │ 681.7 Kitem/s │ 689.7 Kitem/s │         │
│     ├─ 64                                                      502 ns        │ 2.711 µs      │ 1.38 µs       │ 1.369 µs      │ 1024    │ 1048576
│     │                                                          1.991 Mitem/s │ 368.8 Kitem/s │ 724.6 Kitem/s │ 730.2 Kitem/s │         │
│     ╰─ 128                                                     546.4 ns      │ 2.047 µs      │ 1.368 µs      │ 1.346 µs      │ 1024    │ 1048576
│                                                                1.83 Mitem/s  │ 488.3 Kitem/s │ 730.4 Kitem/s │ 742.8 Kitem/s │         │
╰─ push                                                                        │               │               │               │         │
   ├─ colliding                                                                │               │               │               │         │
   │  ├─ DatabaseConfig { shards: 8, optimistic_read: false }    230.1 ns      │ 2.199 µs      │ 1.671 µs      │ 1.64 µs       │ 1024    │ 1048576
   │  │                                                          4.345 Mitem/s │ 454.6 Kitem/s │ 598.2 Kitem/s │ 609.4 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 8, optimistic_read: true }     27.63 ns      │ 1.451 µs      │ 1.335 µs      │ 1.281 µs      │ 1024    │ 1048576
   │  │                                                          36.19 Mitem/s │ 688.8 Kitem/s │ 749 Kitem/s   │ 780.3 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 16, optimistic_read: false }   769.2 ns      │ 1.979 µs      │ 1.71 µs       │ 1.677 µs      │ 1024    │ 1048576
   │  │                                                          1.299 Mitem/s │ 505.3 Kitem/s │ 584.4 Kitem/s │ 596 Kitem/s   │         │
   │  ├─ DatabaseConfig { shards: 16, optimistic_read: true }    27.45 ns      │ 1.678 µs      │ 1.378 µs      │ 1.334 µs      │ 1024    │ 1048576
   │  │                                                          36.42 Mitem/s │ 595.7 Kitem/s │ 725.6 Kitem/s │ 749.5 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 32, optimistic_read: false }   144.1 ns      │ 2.328 µs      │ 1.718 µs      │ 1.678 µs      │ 1024    │ 1048576
   │  │                                                          6.937 Mitem/s │ 429.5 Kitem/s │ 581.8 Kitem/s │ 595.8 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 32, optimistic_read: true }    30.09 ns      │ 1.935 µs      │ 1.321 µs      │ 1.185 µs      │ 1024    │ 1048576
   │  │                                                          33.22 Mitem/s │ 516.6 Kitem/s │ 756.5 Kitem/s │ 843.6 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 64, optimistic_read: false }   495.2 ns      │ 1.963 µs      │ 1.679 µs      │ 1.635 µs      │ 1024    │ 1048576
   │  │                                                          2.019 Mitem/s │ 509.2 Kitem/s │ 595.5 Kitem/s │ 611.2 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 64, optimistic_read: true }    27.51 ns      │ 1.749 µs      │ 1.339 µs      │ 1.255 µs      │ 1024    │ 1048576
   │  │                                                          36.34 Mitem/s │ 571.5 Kitem/s │ 746.7 Kitem/s │ 796.7 Kitem/s │         │
   │  ├─ DatabaseConfig { shards: 128, optimistic_read: false }  136.7 ns      │ 2.319 µs      │ 1.697 µs      │ 1.661 µs      │ 1024    │ 1048576
   │  │                                                          7.31 Mitem/s  │ 431.1 Kitem/s │ 589 Kitem/s   │ 601.6 Kitem/s │         │
   │  ╰─ DatabaseConfig { shards: 128, optimistic_read: true }   27.78 ns      │ 1.542 µs      │ 1.359 µs      │ 1.293 µs      │ 1024    │ 1048576
   │                                                             35.98 Mitem/s │ 648.2 Kitem/s │ 735.6 Kitem/s │ 773.2 Kitem/s │         │
   ╰─ non_colliding                                                            │               │               │               │         │
      ├─ DatabaseConfig { shards: 8, optimistic_read: false }    381.3 ns      │ 5.85 µs       │ 605.2 ns      │ 831.2 ns      │ 1024    │ 1048576
      │                                                          2.622 Mitem/s │ 170.9 Kitem/s │ 1.652 Mitem/s │ 1.202 Mitem/s │         │
      ├─ DatabaseConfig { shards: 8, optimistic_read: true }     351.6 ns      │ 5.603 µs      │ 890 ns        │ 1.066 µs      │ 1024    │ 1048576
      │                                                          2.843 Mitem/s │ 178.4 Kitem/s │ 1.123 Mitem/s │ 938 Kitem/s   │         │
      ├─ DatabaseConfig { shards: 16, optimistic_read: false }   102.4 ns      │ 4.131 µs      │ 375.1 ns      │ 525.7 ns      │ 1024    │ 1048576
      │                                                          9.758 Mitem/s │ 242 Kitem/s   │ 2.665 Mitem/s │ 1.902 Mitem/s │         │
      ├─ DatabaseConfig { shards: 16, optimistic_read: true }    275.5 ns      │ 3.716 µs      │ 495.2 ns      │ 672.5 ns      │ 1024    │ 1048576
      │                                                          3.629 Mitem/s │ 269 Kitem/s   │ 2.018 Mitem/s │ 1.486 Mitem/s │         │
      ├─ DatabaseConfig { shards: 32, optimistic_read: false }   88.6 ns       │ 3.108 µs      │ 289.6 ns      │ 430.3 ns      │ 1024    │ 1048576
      │                                                          11.28 Mitem/s │ 321.6 Kitem/s │ 3.452 Mitem/s │ 2.323 Mitem/s │         │
      ├─ DatabaseConfig { shards: 32, optimistic_read: true }    156.4 ns      │ 2.706 µs      │ 330.6 ns      │ 458.9 ns      │ 1024    │ 1048576
      │                                                          6.392 Mitem/s │ 369.4 Kitem/s │ 3.024 Mitem/s │ 2.178 Mitem/s │         │
      ├─ DatabaseConfig { shards: 64, optimistic_read: false }   87.92 ns      │ 2.054 µs      │ 249.9 ns      │ 357 ns        │ 1024    │ 1048576
      │                                                          11.37 Mitem/s │ 486.6 Kitem/s │ 4 Mitem/s     │ 2.8 Mitem/s   │         │
      ├─ DatabaseConfig { shards: 64, optimistic_read: true }    90.64 ns      │ 2.009 µs      │ 271.8 ns      │ 376 ns        │ 1024    │ 1048576
      │                                                          11.03 Mitem/s │ 497.6 Kitem/s │ 3.679 Mitem/s │ 2.659 Mitem/s │         │
      ├─ DatabaseConfig { shards: 128, optimistic_read: false }  91.73 ns      │ 1.786 µs      │ 244.8 ns      │ 329 ns        │ 1024    │ 1048576
      │                                                          10.9 Mitem/s  │ 559.8 Kitem/s │ 4.084 Mitem/s │ 3.039 Mitem/s │         │
      ╰─ DatabaseConfig { shards: 128, optimistic_read: true }   98.61 ns      │ 1.566 µs      │ 263.5 ns      │ 340.6 ns      │ 1024    │ 1048576
                                                                 10.14 Mitem/s │ 638.1 Kitem/s │ 3.794 Mitem/s │ 2.935 Mitem/s │         │
```
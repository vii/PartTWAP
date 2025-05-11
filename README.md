# PartTWAP - partitioned time weighted average price (TWAP) calculation

Inspired by [Calculating TWAP on BigQuery](https://medium.com/google-cloud/calculating-twap-on-bigquery-b4dc14b82e98) by Mark Scannell, this calculates the TWAP based on data compressed using TurboPFor and other libraries.

This is not a Google repository. It is neither recommended nor endorsed - just a playground for trying these techniques.

## Build

```sh
 docker build -t partvwap .
 docker run --privileged --mount type=bind,source=$(pwd),target=$(pwd) --mount type=bind,source=/tmp,target=/tmp  -it partvwap:latest bash -c "cd $(pwd) && cmake -DCMAKE_BUILD_TYPE=Release -B cmake-build -S . && make -j10 -C cmake-build"
```

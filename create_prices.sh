#!/bin/sh

symbols="AAPL MSFT GOOG AMZN"; n=1000000; phi=0.9; mu=100; sigma=1; \
awk -v syms="$symbols" -v n=$n -v phi=$phi -v mu=$mu -v sigma=$sigma '
BEGIN{
  srand();
  split(syms,S);
  print "symbol,price";
  for(i in S){
    p = mu;
    for(t=1;t<=n;t++){
      eps = sigma*sqrt(-2*log(rand()))*cos(2*3.141592653589793*rand());
      p = mu + phi*(p-mu) + eps;
      printf "%s,%.4f\n", S[i], p;
    }
  }
}' > prices.csv

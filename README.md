Additional deps:

## go get deps to add for lnd
`github.com/andybalholm/brotli`
`github.com/gabstv/go-bsdiff/pkg/bspatch`

## go.mod entries to add for lnd
```
replace github.com/breez/breez => github.com/djkazic/breez v0.0.8

replace github.com/btcsuite/btcwallet/walletdb => github.com/djkazic/btcwallet/walletdb v1.7.4
```

then run `go mod tidy`

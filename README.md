# uniqipgo
Calculates number of unique IP v4 in a file

# Used strategies

## Naive #0
```go run cmd/naive/naive.go -f ip-list.txt -o 0```

Using one big `map[string]bool`. It gives the baseline results.

On my machine for 100mln of addresses it spends 54.5s to count all the addresses

## Naive #1
```go run cmd/naive/naive.go -f ip-list.txt -o 1```

Using `map[uint32]bool`. Logic behind this is to parse string into 4 bytes and then comine them into 1 number

On my machine for 100mln of addresses it spends ~36s to count all the addresses

## Naive #2
```go run cmd/naive/naive.go -f ip-list.txt -o 2```

Using `map[[4]uint8]bool`. No need to convert 4 bytes to number, because arrays are comparable

On my machine for 100mln of addresses it also spends ~36s to count all the addresses

## Tree
```go run cmd/tree/tree.go -f ip-list.txt```

This strategy uses approach where every byte of an IP address is a level of a tree.

For example addresses
1.1.2.1 and 1.1.2.2
Have the same 2 top levels, and then they're sepatate to 2 different branches.

This gives us flexibility to run multiple goroutines in parallel, so they can make changes to the tree on independent branches.

This alrorithms takes ~8.5s on my 12 cores CPU to cound all the IPs in the list of 100mln records
Or ~15s for 200mil IPs

## Array Of Maps
```go run cmd/arrmapstorage/arrmapstorage.go -f ip-list.txt```

This strategy utilizes the same ideas as Tree, but instead of having multiple levels of maps, it just has 256 top level maps. Each of these maps has it's own Mutex, so they are modified in parallel.

This algorithm takes ~6.5s on my machine (with 12 cores) for 100mil IPs
Or ~14s for 200mil IPs

## Fanout
```go run cmd/arrmapstorage/arrmapstorage.go -f ip-list.txt```

Array of maps has 1 issue: if several ips are handled at the same moment and they share equal last octet, they nedd to wait for the Mutex.Lock().

In this strategy we have N maps where map for IP is selected by division remainder of IP's last octet by N. In this case we don't need locks for maps.

# Ignored stategies

## Manual parsing rune by rune
I tried this, but split + atoi worked better for me.

## Multiple scanners that scan at 2+ regions of the initial file
Didn't give speed benefit

## Large bitmap, where each bit represents 1 IP
This is probably faster than my strategies, but for smaller files this strategy takes too much space.

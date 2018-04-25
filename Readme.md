# Lab 2 README

## BinStorage 
BinStorageProxy keeps a list of pre-initialized trib.Storage with NewClient(), and acts as a proxy which returns trib.Storage to each call to Bin(). Bin(name) will hash the name and mod it with number of trib.Storage, and assign a trib.Storage to the user, namely BinStorageClient. 
## Storage
BinStorageClient implements the trib.Stroage interface but instead of re-writing the RPC calls code, it uses the trib.Storage implemented in Lab1 and delegate the work to it. But before it do that, it appends the prefix, for example "Alice::" to the key/pattern, and Escape() some of the key/pattern. After RPC call returns, it simply Unescape() or TrimPrefix() and returns. 
## Keeper 
Keeper simply invoke Clock() on each backend every second and come up with the highest clock returned, and make the synClockNextSecond = maxClockThisSecond + 1. The clock between all servers will converge within 3 seconds given there is an operation on any server that ticks the clock higher at the beginning of the 3 seconds. It uses ticker.C instead of time.Sleep to prevent clock skew. 
## Tribber
Tribber keeps no data structure but the BinStorage, therefore a stateless front-end. 

It will use (not keep it) a Bin('USERS_LIST') for users and uses a key-value store instead of key-list store to prevent concurrent requests and ensure at-most-once semantics. 

It also uses (not keep it) a Bin('USERS_LIST_CACHE') that will have roughly 20 users to cache quick access to ListUser() operation. And such cache is stored in the backend hence making the front-end stateless. Key-value, rather than key-list is used here for similar reason as that of the previous paragraph. 

It also uses (not keep it) a Bin(user) for each user, in which I keep their follow list as key-value store where key is the user and value is "EXISTED". When Unfollow(), simply set value to "" and it will be deleted. This is done for a similar reason as SignUp() to ensure at-most-once semantics when concurrent Follow()/Unfollow(). 

For tribber list, I use the key-list store simply a tribber can be repeated. 

## Miscellaneous
I did some testing on caching and discover that in Lab 1, by storing the connection but never closing or timeout it, it will create too many connections if I were to create a new StorageClient for every call. Hence, I save the connection in the BinStorageProxy to improve this situation. If this BinStorageProxy were to be swapped out with the testing one, it may not stand a pressure test that will request more than 100 calls. 

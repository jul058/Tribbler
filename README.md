# cse223b_lab3

back-end:
Similar to lab2, we store data in back-ends based on the hash result for user name. initially every data has two 
copies on back-ends: one on the current alive back-end that hashed to and one on the successor of that back-end.
In addition, back-end holds two maps: aliveBackends and bitmap.
aliveBackends is for keeper to keep track of what back-ends are alive. It is very useful for handling crash and
join issues. bitmap is a 2-D array that keeps track of what back-end's data the current back-end is holding. 
aliveBackends maps to one bin, but bitmap maps to different bins per back-end.

keeper:
Keeper now has to handle the issue when back-end fails or comes back. It uses the data stored in back-end to achieve
our goal. Every second it will dial each back-end, for those fails, it will check if they are set to alive. If so, it
will enter crash function, do data migration and set alive flag to false. For those can be dialed, it will check if 
their alive flags are set to false. If so, they will be brought to join function and replicate their original data, as
well as data they should store for other back-ends. Then it will set the alive to be true.
Keeper will communicate with each other to know who is leader. Other keepers just need to wait and consistantly check
if leader fails. If a leader fails, we will elect a new leader based on their timestamp in kcconfig.

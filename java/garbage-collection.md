Mark Sweep Compact (MSC) is popular algorithm used in HotSpot by default. It is implemented in STW fashion and has 3 phases:

MARK - traverse live object graph to mark reachable objects
SWEEP - scans memory to find unmarked memory
COMPACT - relocating marked objects to defragment free memory
When relocating objects in the heap, the JVM should correct all references to this object. During the relocation process the object graph is inconsistent, that is why STW pause is required.

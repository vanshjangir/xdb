This database is based on b+trees, and uses copy-on-write mechanism. The problem with using copy-on-write is that, it is not compatible with maintaining a linked list of leaf nodes. As if a leaf node is copied, then its previous leaf node's next pointer must also be updated to point to the copied node's address(offset on the main storage file). But what happens if the changes needs to be rolled back.\
My approach is to maintain a list of the new copies of leaf nodes. Suppose A,B and C are three leaf nodes.\
A.next = B and B.prev = A\
B.next = C and C.prev = B\
When B is updated a new copy is formed , Bp and Bp.next = C and Bpprev = A. Note that A and C are still pointing to B and not Bp.\
We place Bp in a list and after the operation(insertion/deletion/updation), we change the next and prev pointer of A and C.\
Note that this is happening in an ongoing transaction. So what happens when the transaction needs to be rolled back.\ In that case, we also maintain a disk-backed map of the A and C, which points to their older address at the transaction begin time. In this way even if the some crash happens in the ongoing transaction, as long as the transactions is not committed, next and prev pointer of A and C can always be reverted.

#include "data.h"
#include "csapp.h"
#include <string.h>
#include <pthread.h>
#include "store.h"

/*
 * Create a blob with given content and size.
 * The content is copied, rather than shared with the caller.
 * The returned blob has one reference, which becomes the caller's
 * responsibility.
 *
 * @param content  The content of the blob.
 * @param size  The size in bytes of the content.
 * @return  The new blob, which has reference count 1.
 */

//pthread_mutex_t gen_mutex;
//int hashCount = -1;

BLOB *blob_create(char *content, size_t size){
    BLOB* ptr = Malloc(sizeof(BLOB));
    ptr->refcnt = 0;
    ptr->size = size;
    pthread_mutex_t mut;
    pthread_mutex_init(&mut, NULL);
    ptr->mutex = mut;
    char* buf = Malloc(size);
    pthread_mutex_lock(&ptr->mutex);
    memcpy(buf, content, size);
    *(buf + size) = '\0';
    pthread_mutex_unlock(&ptr->mutex);
    //strcpy(buf, content);
    ptr->content = buf;
    ptr->prefix = buf;
    // pthread_mutex_t mut;
    // pthread_mutex_init(&mut, NULL);
    // ptr->mutex = mut;
    //pthread_mutex_init(&gen_mutex, NULL); //init general mutex
    return ptr;
}

/*
 * Increase the reference count on a blob.
 *
 * @param bp  The blob.
 * @param why  Short phrase explaining the purpose of the increase.
 * @return  The blob pointer passed as the argument.
 */
BLOB *blob_ref(BLOB *bp, char *why){
    pthread_mutex_lock(&bp->mutex);
    bp->refcnt +=1;
    pthread_mutex_unlock(&bp->mutex);
    return bp;
}

/*
 * Decrease the reference count on a blob.
 * If the reference count reaches zero, the blob is freed.
 *
 * @param bp  The blob.
 * @param why  Short phrase explaining the purpose of the decrease.
 */
void blob_unref(BLOB *bp, char *why){
    //pthread_mutex_lock(&gen_mutex);
    if(bp != NULL){
        pthread_mutex_lock(&bp->mutex);
        bp->refcnt -=1;
        if(bp->refcnt == 0){
            Free(bp);
            pthread_mutex_unlock(&bp->mutex);
            pthread_mutex_destroy(&bp->mutex);
            return;
        }
        pthread_mutex_unlock(&bp->mutex);
    }
    else
        Free(bp);
}

/*
 * Compare two blobs for equality of their content.
 *
 * @param bp1  The first blob.
 * @param bp2  The second blob.
 * @return 0 if the blobs have equal content, nonzero otherwise.
 */
int blob_compare(BLOB *bp1, BLOB *bp2){
    // if(bp1->refcnt != bp2->refcnt)
    //     return -1;
    // if(bp1->size != bp2->size)
    //     return -1;
    if(strcmp(bp1->content, bp2->content) != 0)
        return -1;
    // if(strcmp(bp1->prefix, bp2->prefix) != 0)
    //     return -1;
    return 0;
}

/*
 * Hash function for hashing the content of a blob.
 *
 * @param bp  The blob.
 * @return  Hash of the blob.
 */
int blob_hash(BLOB *bp){
    //pthread_mutex_lock(&gen_mutex);
    int total=0;
    char* temp = bp->content;
    for(int i=0; i<strlen(bp->content); i++){
        total += *temp;
        temp+=1;
    }
    total = total%(the_map.num_buckets); //length of store;
    //pthread_mutex_unlock(&gen_mutex);
    return total;
}


 /* Create a key from a blob.
 * The key inherits the caller's reference to the blob.
 *
 * @param bp  The blob.
 * @return  the newly created key.
 */

KEY *key_create(BLOB *bp){
    //pthread_mutex_lock(&bp->mutex);
    KEY* ptr = Malloc(sizeof(KEY));
    //bp->refcnt+=1;
    ptr->blob = bp;
    blob_ref(ptr->blob, NULL);
    ptr->hash = blob_hash(bp);
    //pthread_mutex_unlock(&bp->mutex);
    return ptr;
}

/*
 * Dispose of a key, decreasing the reference count of the contained blob.
 * A key must be disposed of only once and must not be referred to again
 * after it has been disposed.
 *
 * @param kp  The key.
 */
void key_dispose(KEY *kp){
    //pthread_mutex_lock(&gen_mutex);
    //kp->blob->refcnt -=1;
    blob_unref(kp->blob, NULL);
    Free(kp);
    //pthread_mutex_unlock(&gen_mutex);
}

/*
 * Compare two keys for equality.
 *
 * @param kp1  The first key.
 * @param kp2  The second key.
 * @return  0 if the keys are equal, otherwise nonzero.
 */
int key_compare(KEY *kp1, KEY *kp2){
    if(kp1->hash != kp2->hash)
        return -1;
    // if(kp1->blob->refcnt != kp2->blob->refcnt)
    //     return -1;
    // if(kp1->blob->size != kp2->blob->size)
    //     return -1;
    // if(strcmp(kp1->blob->content, kp2->blob->content) != 0)
    //     return -1;
    // if(strcmp(kp1->blob->prefix, kp2->blob->prefix) != 0)
    //     return -1;
    return 0;
}

/*
 * Create a version of a blob for a specified creator transaction.
 * The version inherits the caller's reference to the blob.
 * The reference count of the creator transaction is increased to
 * account for the reference that is stored in the version.
 *
 * @param tp  The creator transaction.
 * @param bp  The blob.
 * @return  The newly created version.
 */
VERSION *version_create(TRANSACTION *tp, BLOB *bp){
    VERSION* ptr = Malloc(sizeof(VERSION));
    ptr->creator = tp;
    ptr->blob = bp;
    ptr->next = NULL;
    ptr->prev = NULL;
    //pthread_mutex_lock(&gen_mutex);
    //tp->refcnt +=1;
    trans_ref(tp, NULL);
    if(bp != NULL)
        blob_ref(bp, NULL);//bp->refcnt+=1;
    //pthread_mutex_unlock(&gen_mutex);
    return ptr;
}

/*
 * Dispose of a version, decreasing the reference count of the
 * creator transaction and contained blob.  A version must be
 * disposed of only once and must not be referred to again once
 * it has been disposed.
 *
 * @param vp  The version to be disposed.
 */
void version_dispose(VERSION *vp){
    //pthread_mutex_lock(&gen_mutex);
    blob_unref(vp->blob, NULL);//vp->blob->refcnt -=1;
    trans_unref(vp->creator, NULL);//vp->creator->refcnt -=1;
    Free(vp);
    //pthread_mutex_unlock(&gen_mutex);
}
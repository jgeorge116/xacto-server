#include "csapp.h"
#include "transaction.h"
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>

// typedef enum { TRANS_PENDING, TRANS_COMMITTED, TRANS_ABORTED } TRANS_STATUS;

/*
typedef struct dependency {
  struct transaction *trans;  // Transaction on which the dependency depends.
  struct dependency *next;    // Next dependency in the set.
} DEPENDENCY;

typedef struct transaction {
  unsigned int id;           // Transaction ID.
  unsigned int refcnt;       // Number of references (pointers) to transaction.
  TRANS_STATUS status;       // Current transaction status.
  DEPENDENCY *depends;       // Singly-linked list of dependencies.
  int waitcnt;               // Number of transactions waiting for this one.
  sem_t sem;                 // Semaphore to wait for transaction to commit or abort.
  pthread_mutex_t mutex;     // Mutex to protect fields.
  struct transaction *next;  // Next in list of all transactions
  struct transaction *prev;  // Prev in list of all transactions.
} TRANSACTION;
*/

/*
 * Initialize the transaction manager.
 */
int did_suspend = 0;
TRANSACTION trans_list;

int id_count = 0;

void trans_init(void){
    trans_list.id = id_count;
    trans_list.refcnt = 0;
    trans_list.status = TRANS_PENDING;
    trans_list.depends = NULL;
    trans_list.waitcnt = 0;
    sem_t sem;
    sem_init(&sem,0, 0);
    trans_list.sem = sem;
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);
    trans_list.mutex = mutex;
    trans_list.next = &trans_list;
    trans_list.prev = &trans_list;
}

/*
 * Finalize the transaction manager. Iterate list and free each transaction
 */
void trans_fini(void){
    TRANSACTION* cursor = &trans_list;
    TRANSACTION* temp;
    cursor = cursor->next;
    while(cursor != &trans_list){
        temp = cursor;
        pthread_mutex_destroy(&cursor->mutex);
        sem_destroy(&cursor->sem);
        DEPENDENCY* dep_cursor = cursor->depends; //head of dependencies
        DEPENDENCY* temp_dep = cursor->depends;
        if(dep_cursor != NULL){
            while(dep_cursor != NULL){ //free dependendcies
                temp_dep = dep_cursor;
                Free(dep_cursor);
                dep_cursor = temp_dep->next;
            }
            Free(cursor);
        }
        cursor = temp->next;
    }
}

/*
 * Create a new transaction.
 *
 * @return  A pointer to the new transaction (with reference count 1)
 * is returned if creation is successful, otherwise NULL is returned.
 */
TRANSACTION *trans_create(void){
    TRANSACTION* new_trans = Malloc(sizeof(TRANSACTION));
    new_trans->refcnt = 1;
    new_trans->status = TRANS_PENDING;
    new_trans->depends = NULL;
    new_trans->waitcnt = 0;
    sem_t sem;
    sem_init(&sem,0, 0); //change if necessary
    new_trans->sem = sem;
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);
    new_trans->mutex = mutex;
    pthread_mutex_lock(&trans_list.mutex); //lock count
     id_count+=1;
    pthread_mutex_unlock(&trans_list.mutex);
    new_trans->id = id_count;
    new_trans->prev = trans_list.prev; //link up correctly
    trans_list.prev->next = new_trans;
    new_trans->next = &trans_list;
    trans_list.prev = new_trans;
    return new_trans;
}

/*
 * Increase the reference count on a transaction.
 *
 * @param tp  The transaction.
 * @param why  Short phrase explaining the purpose of the increase.
 * @return  The transaction pointer passed as the argument.
 */
TRANSACTION *trans_ref(TRANSACTION *tp, char *why){
    pthread_mutex_lock(&tp->mutex);
    tp->refcnt +=1;
    pthread_mutex_unlock(&tp->mutex);
    return tp;
}

/*
 * Decrease the reference count on a transaction.
 * If the reference count reaches zero, the transaction is freed.
 *
 * @param tp  The transaction.
 * @param why  Short phrase explaining the purpose of the decrease.
 */
void trans_unref(TRANSACTION *tp, char *why){
    pthread_mutex_lock(&tp->mutex);
    tp->refcnt-=1;
    pthread_mutex_unlock(&tp->mutex);
    if(tp->refcnt == 0){
        TRANSACTION* cursor = &trans_list;
        while(cursor->id != tp->id){ //get proper one, remove from list & free
            cursor = cursor->next;
        }
        cursor->prev->next = cursor->next;
        cursor->next->prev = cursor->prev;
        cursor->next = NULL;
        cursor->prev = NULL;
        Free(cursor);
    }
}

/*
 * Add a transaction to the dependency set for this transaction.
 *
 * @param tp  The transaction to which the dependency is being added.
 * @param dtp  The transaction that is being added to the dependency set.
 */
void trans_add_dependency(TRANSACTION *tp, TRANSACTION *dtp){
    pthread_mutex_lock(&dtp->mutex);
    TRANSACTION* cursor = &trans_list;
    tp->refcnt +=1;
    while(cursor->id != dtp->id) //gets proper transaction
        cursor = cursor->next;
    if(cursor->depends == NULL){ //add to head if none exists
        DEPENDENCY* head = Malloc(sizeof(DEPENDENCY));
        head->trans = tp;
        head->next = NULL;
        cursor->depends = head;
        //cursor->waitcnt +=1;
        tp->waitcnt +=1;
    }
    else{
        DEPENDENCY* depend_cursor = dtp->depends; //head of dependencies
        while(depend_cursor->next != NULL)
            depend_cursor = depend_cursor->next;
        DEPENDENCY* new_dependency = Malloc(sizeof(DEPENDENCY));
        new_dependency->trans = tp;
        new_dependency->next = NULL;
        depend_cursor->next = new_dependency;
        //cursor->waitcnt +=1;
        tp->waitcnt +=1;
    }
    pthread_mutex_unlock(&dtp->mutex);
}

/*
 * Try to commit a transaction.  Committing a transaction requires waiting
 * for all transactions in its dependency set to either commit or abort.
 * If any transaction in the dependency set abort, then the dependent
 * transaction must also abort.  If all transactions in the dependency set
 * commit, then the dependent transaction may also commit.
 *
 * In all cases, this function consumes a single reference to the transaction
 * object.
 *
 * @param tp  The transaction to be committed.
 * @return  The final status of the transaction: either TRANS_ABORTED,
 * or TRANS_COMMITTED.
 */
TRANS_STATUS trans_commit(TRANSACTION *tp){
    // if(tp->waitcnt == 0 && tp->depends != NULL){
    //     return trans_abort(tp);
    // }
    TRANSACTION* temp = &trans_list;
    int count = 0;
    temp = temp->next;
    while(temp->next != &trans_list){
        count +=1;
        temp = temp->next;
    }
     int did_suspend_real = 0;
     //int did_suspend = 0;
    while(tp->waitcnt != 0){
        pthread_mutex_lock(&tp->mutex);
        did_suspend = 1;
        pthread_mutex_unlock(&tp->mutex);
        //did_suspend = 1;
        P(&tp->sem);
        did_suspend_real = 1;
        if(tp->status == TRANS_ABORTED)
            return trans_abort(tp);
        pthread_mutex_lock(&tp->mutex);
        tp->waitcnt-=1;
        pthread_mutex_unlock(&tp->mutex);
    }

    // if(did_suspend_real){
    //     pthread_mutex_lock(&tp->mutex);
    //     did_suspend = 1;
    //     pthread_mutex_unlock(&tp->mutex);
    // }
    if(!did_suspend && !did_suspend_real && (count!=0)){
        // pthread_mutex_lock(&tp->mutex);
        // did_suspend = 1;
        // pthread_mutex_unlock(&tp->mutex);
        return trans_abort(tp);
    }
    // pthread_mutex_lock(&tp->mutex);
    // did_suspend = 0;
    // pthread_mutex_unlock(&tp->mutex);
    DEPENDENCY* dep_cur = tp->depends;
    while(dep_cur != NULL){ //alert all
        V(&dep_cur->trans->sem);
        dep_cur = dep_cur->next;
    }
    pthread_mutex_lock(&tp->mutex);
    tp->status = TRANS_COMMITTED;
    pthread_mutex_unlock(&tp->mutex);
    trans_unref(tp, NULL);
    return tp->status;
}

/*
 * Abort a transaction.  If the transaction has already committed, it is
 * a fatal error and the program crashes.  If the transaction has already
 * aborted, no change is made to its state.  If the transaction is pending,
 * then it is set to the aborted state, and any transactions dependent on
 * this transaction must also abort.
 *
 * In all cases, this function consumes a single reference to the transaction
 * object.
 *
 * @param tp  The transaction to be aborted.
 * @return  TRANS_ABORTED.
 */
TRANS_STATUS trans_abort(TRANSACTION *tp){
    if(tp->status == TRANS_COMMITTED){
        fprintf(stderr,"Trying to abort a commited transaction\n");
        abort();
    }
    else if(tp->status == TRANS_ABORTED){
        return TRANS_ABORTED;
    }
    else{ //must be pending
        pthread_mutex_lock(&tp->mutex);
        tp->status = TRANS_ABORTED;
        DEPENDENCY* cur = tp->depends;
        while(cur != NULL){
            cur->trans->status = TRANS_ABORTED;
            tp->waitcnt-=1;
            V(&cur->trans->sem);
            cur = cur->next;
        }
        pthread_mutex_unlock(&tp->mutex);
        trans_unref(tp, NULL);
    }
    return tp->status;
}

/*
 * Get the current status of a transaction.
 * If the value returned is TRANS_PENDING, then we learn nothing,
 * because unless we are holding the transaction mutex the transaction
 * could be aborted at any time.  However, if the value returned is
 * either TRANS_COMMITTED or TRANS_ABORTED, then that value is the
 * stable final status of the transaction.
 *
 * @param tp  The transaction.
 * @return  The status of the transaction, as it was at the time of call.
 */
TRANS_STATUS trans_get_status(TRANSACTION *tp){
    return tp->status;
}

char* getStatusValue(TRANS_STATUS tp){
    switch(tp){
        case TRANS_PENDING:
            return "TRANSACTION PENDING";
        case  TRANS_ABORTED:
            return "TRANSACTION ABORTED";
        case TRANS_COMMITTED:
            return "TRANSACTION COMMITED";
    }
    return  NULL;
}

/*
 * Print information about a transaction to stderr.
 * No locking is performed, so this is not thread-safe.
 * This should only be used for debugging.
 *
 * @param tp  The transaction to be shown.
 */
void trans_show(TRANSACTION *tp){
    fprintf(stdout,"Transaction info:\nid: %d\nref count: %d\nstatus %s\nID's of transaction dependencies: ",
        tp->id, tp->refcnt, getStatusValue(tp->status));
    /* loop through dependencies*/
    DEPENDENCY* cursor = tp->depends;
    if(cursor != NULL){
        while(cursor->next != NULL){
            fprintf(stdout, "%d ", cursor->trans->id);
            cursor = cursor->next;
        }
    }
    else{
        fprintf(stdout, "NULL");
    }
    fprintf(stdout,"\nwait count: %d\nnext ptr: %p\nprev ptr: %p", tp->waitcnt, tp->next, tp->prev);
}

/*
 * Print information about all transactions to stderr.
 * No locking is performed, so this is not thread-safe.
 * This should only be used for debugging.
 */
void trans_show_all(void){
    TRANSACTION* cursor = &trans_list;
    while(cursor->next != &trans_list){
        trans_show(cursor);
        fprintf(stdout, "\n"); //for spacing
        cursor = cursor->next;
    }
}
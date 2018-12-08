#include "csapp.h"
#include "client_registry_implementation.h"
#include "client_registry.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/select.h>
#include <sys/socket.h>
/*
 * Initialize a new client registry.
 *
 * @return  the newly initialized client registry.
 */
CLIENT_REGISTRY *creg_init(){
    CLIENT_REGISTRY* ptr = Malloc(sizeof(CLIENT_REGISTRY));
    pthread_mutex_t mut;
    pthread_mutex_init(&mut, NULL);
    ptr->the_mutex = mut;
    sem_t sem;
    sem_init(&sem,0, 0);
    ptr->waitSem = sem;
    fd_set theSet;
    FD_ZERO(&theSet);
    ptr->clientfd_set = theSet;
    ptr->client_fdMax = 1024;
    ptr->clientCounter = 0;
    ptr->waitCounter = 0;
    return ptr;
}

/*
 * Finalize a client registry.
 *
 * @param cr  The client registry to be finalized, which must not
 * be referenced again.
 */
void creg_fini(CLIENT_REGISTRY *cr){
    Free(cr);
}

/*
 * Register a client file descriptor.
 *
 * @param cr  The client registry.
 * @param fd  The file descriptor to be registered.
 */
void creg_register(CLIENT_REGISTRY *cr, int fd){
    pthread_mutex_lock(&(cr->the_mutex));
    cr->clientCounter +=1;
    FD_SET(fd, &(cr->clientfd_set));
    pthread_mutex_unlock(&(cr->the_mutex));
}

/*
 * Unregister a client file descriptor, alerting anybody waiting
 * for the registered set to become empty. (AKA wait for empty)
 *
 * @param cr  The client registry.
 * @param fd  The file descriptor to be unregistered.
 */
void creg_unregister(CLIENT_REGISTRY *cr, int fd){
    pthread_mutex_lock(&(cr->the_mutex));
    FD_CLR(fd, &(cr->clientfd_set));
    cr->clientCounter-=1;
    //if(cr->clientCounter == 0)
       //V(&(cr->waitSem)); //increm sem, alerts creg_wait_for_empty
    pthread_mutex_unlock(&(cr->the_mutex));
    if(cr->clientCounter == 0)
        V(&(cr->waitSem)); //increm sem, alerts creg_wait_for_empty
}

/*
 * A thread calling this function will block in the call until
 * the number of registered clients has reached zero, at which
 * point the function will return.
 *
 * @param cr  The client registry.
 */
void creg_wait_for_empty(CLIENT_REGISTRY *cr){
    P(&(cr->waitSem)); //suspends thread until sem is non zero
    //nothing, goes here if client count hits 0
    //V(&(cr->waitSem));
}

/*
 * Shut down all the currently registered client file descriptors.
 *
 * @param cr  The client registry.
 */
void creg_shutdown_all(CLIENT_REGISTRY *cr){
    for(int i=0; i<FD_SETSIZE; i++)
        if(FD_ISSET(i, &(cr->clientfd_set)))
            shutdown(i, SHUT_RD);
}
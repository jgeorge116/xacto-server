#include "client_registry.h"
#include <sys/select.h>
#include <pthread.h>
#include <semaphore.h>

/*
 * A client registry keeps track of the file descriptors for clients
 * that are currently connected.  Each time a client connects,
 * its file descriptor is added to the registry.  When the thread servicing
 * a client is about to terminate, it removes the file descriptor from
 * the registry.  The client registry also provides a function for shutting
 * down all client connections and a function that can be called by a thread
 * that wishes to wait for the client count to drop to zero.  Such a function
 * is useful, for example, in order to achieve clean termination:
 * when termination is desired, the "main" thread will shut down all client
 * connections and then wait for the set of registered file descriptors to
 * become empty before exiting the program.
 */
typedef struct client_registry {
    pthread_mutex_t the_mutex;
    sem_t waitSem;
    fd_set clientfd_set;
    int client_fdMax;
    int clientCounter;
    int waitCounter;
}CLIENT_REGISTRY;
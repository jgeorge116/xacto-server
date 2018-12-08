#include "debug.h"
#include "client_registry.h"
#include "transaction.h"
#include "store.h"
#include "csapp.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "server.h"
#include <unistd.h>
#include <signal.h>
#include <pthread.h>

static void terminate(int status);

CLIENT_REGISTRY *client_registry;
int got_SIGHUP=0;

void SIGHUP_handler(){
    //got_SIGHUP = 1;
    terminate(EXIT_SUCCESS);
}

int is_number(char c){
    if((c <= '9') && (c >= '0'))
        return 1;
    else
        return 0;
}

int main(int argc, char* argv[]){
    Signal(SIGHUP, SIGHUP_handler);
    // Option processing should be performed here.
    // Option '-p <port>' is required in order to specify the port number
    // on which the server should listen.

    // Perform required initializations of the client_registry,
    // transaction manager, and object store.
    if(argc < 2){
        fprintf(stderr, "enter port number\n");
        exit(EXIT_FAILURE);
    }
    if(strcmp(argv[1], "-p") != 0){
        fprintf(stderr, "undefined command\n");
        exit(EXIT_FAILURE);
    }
    if(argv[2] == NULL){
        fprintf(stderr, "port number not valid\n");
        exit(EXIT_FAILURE);
    }
    char* port_number = argv[2];
    while(*argv[2] != '\0'){
        if(!is_number(*argv[2]++)){
            fprintf(stderr, "port number not valid\n");
            exit(EXIT_FAILURE);
        }
    }

    // struct sigaction ac_struct;
    // memset(&ac_struct, 0, sizeof(ac_struct));
    // ac_struct.sa_handler = SIGHUP_handler;
    // sigaction(SIGHUP, &ac_struct, NULL);
    client_registry = creg_init();
    trans_init();
    store_init();

    int* connected_fd;
    socklen_t client_size = sizeof(struct sockaddr_in);
    int listening_fd = Open_listenfd(port_number);
    struct sockaddr_in client_addr;
    pthread_t tid;
    while(1){
        connected_fd = Malloc(sizeof(int));
        *connected_fd = Accept(listening_fd, (SA*)&client_addr, &client_size);
        if(*connected_fd == -1){
            free(connected_fd);
            terminate(EXIT_SUCCESS);
        }
        Pthread_create(&tid, NULL, xacto_client_service, connected_fd);
        // if(got_SIGHUP){
        //     terminate(EXIT_SUCCESS);
        // }
    }
}

/*
 * Function called to cleanly shut down the server.
 */
void terminate(int status) {
    // Shutdown all client connections.
    // This will trigger the eventual termination of service threads.
    creg_shutdown_all(client_registry);

    debug("Waiting for service threads to terminate...");
    creg_wait_for_empty(client_registry);
    debug("All service threads terminated.");

    // Finalize modules.
    creg_fini(client_registry);
    trans_fini();
    store_fini();

    debug("Xacto server terminating");
    exit(status);
}

#include "csapp.h"
#include "server.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "client_registry.h"
#include "client_registry_implementation.h"
#include "transaction.h"
#include "protocol.h"
#include  <time.h>
#include "data.h"
#include "store.h"


 /* Thread function for the thread that handles client requests.
 *
 * @param  Pointer to a variable that holds the file descriptor for
 * the client connection.  This pointer must be freed once the file
 * descriptor has been retrieved.
 *detach thread DONE
 *free storage of fd DONE
 *close fd DONE
 *register the client file descriptor with the client registry DONE
 *transaction should be created to be used as the context for carrying out client requests DONE
 *enter a service loop in which it repeatedly
  receives a request packet sent by the client, carries out the request,
  and sends a reply packet, followed (in the case of a reply to a GET request)
  by a data packet that contains the result
 *If as a result of carrying out any of the requests, the transaction commits or aborts, then
  (after sending the required reply to the current request) the service
  loop should end and the client service thread terminate, closing the
  client connection
  */

CLIENT_REGISTRY *client_registry;

void send_abort_pkt(int sent_fd){
  XACTO_PACKET* new_reply_pkt = Malloc(sizeof(XACTO_PACKET));
  new_reply_pkt->type =XACTO_REPLY_PKT;
  new_reply_pkt->status = 2;
  new_reply_pkt->null = 1;
  new_reply_pkt->size = 0;
  time_t time_sec;
  struct timespec st_time;
  clock_gettime(CLOCK_MONOTONIC, &st_time);
  time_sec = st_time.tv_sec;
  long int nano = st_time.tv_nsec;
  new_reply_pkt->timestamp_sec = time_sec;
  new_reply_pkt->timestamp_nsec = nano;
  proto_send_packet(sent_fd, new_reply_pkt, NULL);
  Free(new_reply_pkt);
}

void *xacto_client_service(void *arg){
    Pthread_detach(pthread_self());
    int sent_fd = *((int*)(arg));
    Free(arg);
    creg_register(client_registry, sent_fd);
    TRANSACTION* trans_ptr = trans_create();
    if(trans_ptr != NULL){
    while(1){
        XACTO_PACKET gen_pkt;
        XACTO_PACKET* new_pkt = &gen_pkt;// = Malloc(sizeof(XACTO_PACKET));
        void** payload_data = Malloc(512);
        if(proto_recv_packet(sent_fd, new_pkt, NULL) == 0){ //XACTO_PUT_PKT, XACTO_GET_PKT, XACTO_COMMIT_PKT
            if(new_pkt->type == XACTO_PUT_PKT){
                //XACTO_PACKET new_gen_key_pkt;
                XACTO_PACKET* new_key_packet = Malloc(sizeof(XACTO_PACKET));//&new_gen_key_pkt;
                if(proto_recv_packet(sent_fd, new_key_packet, payload_data) == -1){ //get key
                  trans_abort(trans_ptr);
                  Free(payload_data);
                  Free(new_key_packet);
                  send_abort_pkt(sent_fd);
                  break;
                }
                BLOB* new_blob = blob_create(*payload_data, new_key_packet->size); //new blob for key
                KEY* new_key = key_create(new_blob);
                //XACTO_PACKET new_gen_val_pkt;
                XACTO_PACKET* new_value_packet = Malloc(sizeof(XACTO_PACKET));// &new_gen_val_pkt;
                if(proto_recv_packet(sent_fd, new_value_packet, payload_data) == -1){ //get value
                  trans_abort(trans_ptr);
                  Free(payload_data);
                  Free(new_key_packet);
                  Free(new_value_packet);
                  send_abort_pkt(sent_fd);
                  break;
                }
                BLOB* new_blob_value = blob_create(*payload_data, new_value_packet->size); //new blob for value
                TRANS_STATUS flag = store_put(trans_ptr, new_key, new_blob_value); //put
                if(flag == TRANS_ABORTED){
                  //trans_abort(trans_ptr);
                  Free(payload_data);
                  Free(new_key_packet);
                  Free(new_value_packet);
                  send_abort_pkt(sent_fd);
                  break;
                }

                XACTO_PACKET* new_reply_pkt = Malloc(sizeof(XACTO_PACKET));
                new_reply_pkt->type =XACTO_REPLY_PKT;
                new_reply_pkt->status = 0;
                new_reply_pkt->null = 1;
                new_reply_pkt->size = 0;
                time_t time_sec;
                struct timespec st_time;
                clock_gettime(CLOCK_MONOTONIC, &st_time);
                time_sec = st_time.tv_sec;
                long int nano = st_time.tv_nsec;
                new_reply_pkt->timestamp_sec = time_sec;
                new_reply_pkt->timestamp_nsec = nano;
                if(proto_send_packet(sent_fd, new_reply_pkt, NULL) == -1){
                  trans_abort(trans_ptr);
                  Free(new_reply_pkt);
                  Free(payload_data);
                  Free(new_key_packet);
                  Free(new_value_packet);
                  send_abort_pkt(sent_fd);
                  break;
                }

                Free(new_reply_pkt);
                Free(payload_data);
                Free(new_key_packet);
                Free(new_value_packet);
                store_show();
            }
            else if(new_pkt->type == XACTO_GET_PKT){
                //XACTO_PACKET new_gen_key_pkt;
                XACTO_PACKET* new_key_packet = Malloc(sizeof(XACTO_PACKET));//&new_gen_key_pkt;
                if(proto_recv_packet(sent_fd, new_key_packet, payload_data) == -1){ //get key
                  trans_abort(trans_ptr);
                  Free(payload_data);
                  Free(new_key_packet);
                  send_abort_pkt(sent_fd);
                  break;
                }
                BLOB* new_blob = blob_create(*payload_data, new_key_packet->size); //new blob for key
                KEY* new_key = key_create(new_blob);
                BLOB get_val;
                BLOB* ptr_get= &get_val;
                //BLOB** double_ptr = &ptr_get;
                // store_get(trans_ptr, new_key, double_ptr);
                XACTO_PACKET* new_reply_pkt = Malloc(sizeof(XACTO_PACKET)); //reply packet
                new_reply_pkt->type =XACTO_REPLY_PKT;
                new_reply_pkt->status = 0;
                new_reply_pkt->null = 1;
                new_reply_pkt->size = 0;
                time_t time_sec;
                struct timespec st_time;
                clock_gettime(CLOCK_MONOTONIC, &st_time);
                time_sec = st_time.tv_sec;
                long int nano = st_time.tv_nsec;
                new_reply_pkt->timestamp_sec = time_sec;
                new_reply_pkt->timestamp_nsec = nano;

                XACTO_PACKET* new_data_pkt = Malloc(sizeof(XACTO_PACKET)); //data packet
                new_data_pkt->type =XACTO_DATA_PKT;
                new_data_pkt->status = 0;
                new_data_pkt->null = 0;
                //new_data_pkt->size = new_key_packet->size;
                time_t data_time_sec;
                struct timespec data_st_time;
                clock_gettime(CLOCK_MONOTONIC, &data_st_time);
                data_time_sec = data_st_time.tv_sec;
                long int data_nano = data_st_time.tv_nsec;
                new_data_pkt->timestamp_sec = data_time_sec;
                new_data_pkt->timestamp_nsec = data_nano;
                TRANS_STATUS flag = store_get(trans_ptr, new_key, &ptr_get);
                if(flag == TRANS_ABORTED){
                  trans_abort(trans_ptr);
                  Free(new_reply_pkt);
                  Free(payload_data);
                  Free(new_data_pkt);
                  Free(new_key_packet);
                  send_abort_pkt(sent_fd);
                  break;
                }
                //char val[strlen(ptr_get->content)];
                // char* val = Malloc(strlen(ptr_get->content));
                char* val;
                if(ptr_get != NULL){
                    val = Malloc(strlen(ptr_get->content));
                    memcpy(val, ptr_get->content, strlen(ptr_get->content));//val = ptr_get->content;
                    val[strlen(ptr_get->content)] = '\0';
                    new_data_pkt->size =strlen(ptr_get->content);
                  }
                  else{
                    char* temp = "null";
                    val = Malloc(strlen(temp));
                    memcpy(val, temp, strlen(temp));
                    new_data_pkt->size = strlen(temp);
                  }
                  // new_data_pkt->size =strlen(ptr_get->content);

                if(proto_send_packet(sent_fd, new_reply_pkt, NULL) == -1 ||
                    proto_send_packet(sent_fd, new_data_pkt, val) == -1){ //send data packet
                  Free(val);
                  trans_abort(trans_ptr);
                  Free(new_reply_pkt);
                  Free(payload_data);
                  Free(new_data_pkt);
                  Free(new_key_packet);
                  send_abort_pkt(sent_fd);
                  break;
                }

                Free(val);
                Free(new_data_pkt);
                Free(new_reply_pkt);
                Free(payload_data);
                Free(new_key_packet);
                store_show();
            }
            else if(new_pkt->type == XACTO_COMMIT_PKT){
                XACTO_PACKET* new_reply_pkt = Malloc(sizeof(XACTO_PACKET));

                TRANS_STATUS temp= trans_commit(trans_ptr);
                if(temp == TRANS_ABORTED){
                  trans_abort(trans_ptr);
                  Free(new_reply_pkt);
                  send_abort_pkt(sent_fd);
                  break;
                }
                new_reply_pkt->type =XACTO_REPLY_PKT;
                new_reply_pkt->status = 1;
                new_reply_pkt->null = 1;
                new_reply_pkt->size = 0;
                time_t time_sec;
                struct timespec st_time;
                clock_gettime(CLOCK_MONOTONIC, &st_time);
                time_sec = st_time.tv_sec;
                long int nano = st_time.tv_nsec;
                new_reply_pkt->timestamp_sec = time_sec;
                new_reply_pkt->timestamp_nsec = nano;
                if(proto_send_packet(sent_fd, new_reply_pkt, NULL) == -1){
                  trans_abort(trans_ptr);
                  Free(new_reply_pkt);
                  break;
                }
                Free(new_reply_pkt);
                // TRANS_STATUS temp= trans_commit(trans_ptr);
                // if(temp == TRANS_ABORTED){
                //   trans_abort(trans_ptr);
                //   Free(new_reply_pkt);
                //   send_abort_pkt(sent_fd);
                //   break;
                // }

                break;
            }
        }
        else{
            Free(payload_data);
            trans_abort(trans_ptr);
            break;
        }
    }
    creg_unregister(client_registry, sent_fd);
    Close(sent_fd);
  }
  else{
    creg_unregister(client_registry, sent_fd);
    Close(sent_fd);
  }
    return NULL;

}
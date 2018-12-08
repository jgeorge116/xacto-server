#include "csapp.h"
#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>

int proto_send_packet(int fd, XACTO_PACKET *pkt, void *data){
    int header_size = (int)sizeof(*pkt);
    uint32_t temp = pkt->size;
    pkt->size = htonl(pkt->size);
    pkt->timestamp_sec = htonl(pkt->timestamp_sec);
    pkt->timestamp_nsec = htonl(pkt->timestamp_nsec);
    int to_return = rio_writen(fd, pkt, header_size);
    if(to_return == -1) //errno set
        return -1;
    else if(to_return > 0){
        if(pkt->size != 0 && data !=NULL){ //get payload size, pkt->size != 0
            int return_payload;
            if(*((char*)data) == '\0'){
                data = "null";
                return_payload = rio_writen(fd, data, temp);
            }
            else
                return_payload = rio_writen(fd, data, temp);
            if(return_payload == -1) //errno
                return -1;
        }
    }
    return 0;
}

int proto_recv_packet(int fd, XACTO_PACKET *pkt, void **datap){
    int header_size = (int)sizeof(*pkt);
    int to_return = rio_readn(fd, pkt, header_size);
    if(to_return == -1 || to_return == 0) //errno set or EOF reached
        return -1;
    else if(to_return > 0){ //reverse then check payload, reading done
        pkt->size = ntohl(pkt->size);
        pkt->timestamp_sec = ntohl(pkt->timestamp_sec);
        pkt->timestamp_nsec = ntohl(pkt->timestamp_nsec);
        if(pkt->size != 0){ //get payload size
            *datap = Malloc(pkt->size);
            int return_payload = rio_readn(fd, *datap, pkt->size);
            //*(((char*)datap)+pkt->size) = '\0'; //TEST
            if(return_payload == -1 || return_payload == 0) //errno or EOF
                return -1;
        }
        else
            pkt->null = 1;
    }
    return 0; //done
}
#include <stdio.h>

#include "networking/execute.h"
#include "networking/msg.h"
#include "start.h"

#define BAR printf("=================================================\n")

void execute(Msg msg, int senderId) {
    BAR;
    printf("Message recieved from %d:\n", senderId);
    printMsg(msg);
    BAR;
}

int main(int argc, char **argv) { return start(argc, argv); }

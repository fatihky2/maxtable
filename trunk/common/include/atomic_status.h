#ifndef ATOMIC_H_
#define ATOMIC_H_


#include "buffer.h"

#define NONKEPT 0
#define KEPT 1

void change_status(BUF *buffer, int curr, int dest);

void change_value_add(int src, int delta);


void change_value_sub(int src, int delta);

#endif


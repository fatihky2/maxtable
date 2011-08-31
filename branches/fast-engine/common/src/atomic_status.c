#include <unistd.h>


#include "atomic_status.h"
#include "buffer.h"

int change_status(BUF *buffer, int curr, int dest)
{
	int count = 0;
    while(!__sync_bool_compare_and_swap(&(buffer->bsstab->atomic_stat), curr, dest))
    {
    	count ++;
        usleep(100);
    }
    return count;
}

void change_value_add(int *src, int delta)
{
    __sync_fetch_and_add(src, delta);
}

void change_value_sub(int *src, int delta)
{
    __sync_fetch_and_sub(src, delta);
}


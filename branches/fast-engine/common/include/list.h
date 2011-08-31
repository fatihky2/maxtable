/*
** list.h 2010-06-17 xueyingfei
**
** Copyright flying/xueyingfei.
**
** This file is part of MaxTable.
**
** Licensed under the Apache License, Version 2.0
** (the "License"); you may not use this file except in compliance with
** the License. You may obtain a copy of the License at
**
** http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
** implied. See the License for the specific language governing
** permissions and limitations under the License.
*/

#ifndef LIST_H_
#define LIST_H_

# ifdef __cplusplus
extern "C" {
# endif


typedef struct link
{
        struct link *next;
        struct link *prev;
} LINK;

typedef struct params_link
{
	LINK    link;
	char    *data;
}PLINK;

#define INITQUE(q) ((LINK *) (q))->next = ((LINK *) (q))->prev = (LINK *) (q)

# define INSQUE(q, l) \
	do {			\
		((LINK *) (l))->next = ((LINK *) (q))->next; \
		((LINK *) (l))->prev = (LINK *) (q); \
		((LINK *) (l))->next->prev \
		 = ((LINK *) (l))->prev->next \
		 = (LINK *) (l);	\
	} while (0)

/* Simplify insert at head */
# define INSQHEAD(q, l)	INSQUE(q, l)

/* Simplify insert at tail */
#define INSQTAIL(q, l)	INSQUE(((LINK *) (q))->prev, l)

#define REMQUE(q, l, t) \
	(	(l) = (t *) (q), \
		((LINK *) (l))->next->prev = ((LINK *) (l))->prev, \
		((LINK *) (l))->prev->next = ((LINK *) (l))->next, \
		INITQUE(l))

/* Simplify removal at head */
#define REMQHEAD(q, l, t)	REMQUE(((LINK *) (q))->next, l, t)

/* Simplify removal at tail */
#define REMQTAIL(q, l, t)	REMQUE(((LINK *) (q))->prev, l, t)

/* For loop for the queue */
#define FOR_QUEUE(type, q, item) for (item = (type *)((LINK *) (q))->next; \
					item != (type *) (q); \
					item = (type *)((LINK *) (item))->next)

/* NEXT element in a queue */
#define QUE_NEXT(q)	((LINK *) (q))->next

/* PREVIOUS element in a queue */
#define QUE_PREV(q)	((LINK *) (q))->prev

#define EMPTYQUE(q)	(((LINK *) (q))->next == (LINK *) (q))

#define JOINQUE(q, l) ( ((LINK *) (l))->prev->next = ((LINK *) (q))->next, \
			((LINK *) (q))->next->prev = ((LINK *) (l))->prev, \
			((LINK *) (l))->prev = (LINK *) (q), \
			((LINK *) (q))->next = (LINK *) (l) )

#define MOVEQUE(q, l)   						\
do { 									\
	LINK *tmpq_; 							\
	LINK *linkq_; 							\
	LINK *ttail_; 							\
									\
	/* If there is anything to be moved */ 				\
	if (!EMPTYQUE((LINK *) (l))){					\
		/* Point to the first item after the head */		\
	        tmpq_ = QUE_NEXT((LINK *) (l)); 			\
									\
		/* Remove the head itself */				\
	        REMQUE((LINK *) (l), linkq_, LINK); 			\
									\
		/* Now add the elements from the source to dest */	\
	        ttail_ = ((LINK *) (q))->prev; 				\
	        JOINQUE(ttail_, tmpq_); 				\
									\
		/* Initialize source head as it is empty now */		\
	        INITQUE(linkq_); 					\
	} 								\
} while (0)


# ifdef __cplusplus
}
# endif


#endif

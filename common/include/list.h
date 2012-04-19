/*
** Copyright (C) 2011 Xue Yingfei
**
** This file is part of MaxTable.
**
** Maxtable is free software: you can redistribute it and/or modify
** it under the terms of the GNU General Public License as published by
** the Free Software Foundation, either version 3 of the License, or
** (at your option) any later version.
**
** Maxtable is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with Maxtable. If not, see <http://www.gnu.org/licenses/>.
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

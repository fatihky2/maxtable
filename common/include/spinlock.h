/*
** spinlock.h 2010-11-18 xueyingfei
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


#ifndef	SPINLOCK_H_
#define SPINLOCK_H_


typedef	pthread_mutex_t	SPINLOCK;

typedef pthread_cond_t	SIGNAL;
//#define	SPINLOCK	pthread_mutex_t

#define	SPINLOCK_INIT(lock)	pthread_mutex_init(&lock, NULL)

#define	P_SPINLOCK(lock)	pthread_mutex_lock(&lock)

#define	V_SPINLOCK(lock)	pthread_mutex_unlock(&lock)







//	spinlock solution  
//	--------------
//#define SPINLOCK	spinlock_t

//typedef spinlock_t	SPINLOCK;

//#define	SPINLOCK_INIT(lock)	spin_lock_init(&lock)

//#define	P_SPINLOCK(lock)	spin_lock(&lock)

//#define	V_SPINLOCK(lock)	spin_unlock(&lock)


/*

spinlock_t lock;        //define spinlock
spin_lock_init(&lock);
spin_lock(&lock);   
.......        //critical section
spin_unlock(&lock);  


*/

#endif

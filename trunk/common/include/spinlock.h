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

#ifndef	SPINLOCK_H_
#define SPINLOCK_H_


typedef	pthread_mutex_t	SPINLOCK;
typedef pthread_cond_t	SIGNAL;
typedef pthread_mutexattr_t 	LOCKATTR;


//#define	SPINLOCK	pthread_mutex_t

#define SPINLOCK_ATTR_INIT(attr)	pthread_mutexattr_init(&attr)

#define SPINLOCK_ATTR_SETTYPE(attr, type)	pthread_mutexattr_settype(&attr, type)

#define	SPINLOCK_INIT(lock, attr)	pthread_mutex_init(&lock, attr)

#define	P_SPINLOCK(lock)	pthread_mutex_lock(&lock)

#define	V_SPINLOCK(lock)	pthread_mutex_unlock(&lock)

#define SPINLOCK_DESTROY(lock)	pthread_mutex_destroy(&lock)








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

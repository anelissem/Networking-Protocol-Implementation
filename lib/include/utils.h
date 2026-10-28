#pragma once

#include <stdint.h>
#include <cstdio>

/* ############# USEFUL MACROS ########## */
#ifdef DEBUG
#define DEBUG_PRINT(fmt, args...)    fprintf(stderr, fmt, ## args)
#else
#define DEBUG_PRINT(fmt, args...)    /* Don't do anything in release builds */
#endif

/* Can be used to update the timeout - returns milliseconds (minimum 50ms) */
#define TIMEOUT_SEND(delay) (delay == 0 ? 100 : ((delay / 2 + 30) < 50 ? 50 : (delay / 2 + 30)))
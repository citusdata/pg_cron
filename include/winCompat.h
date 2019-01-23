#pragma once

#ifndef WINDOWS_INCLUDED
#define WINDOWS_INCLUDED

#if defined(WIN32) || defined(WIN64) 
	#define snprintf _snprintf 
	#define vsnprintf _vsnprintf 
	#define strcasecmp _stricmp 
	#define strncasecmp _strnicmp 
	#define uint unsigned int
	
#endif

typedef int uid_t;
typedef int gid_t;


#endif /* Ndef WINDOWS_INCLUDED */



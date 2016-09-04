/* Copyright 1988,1990,1993,1994 by Paul Vixie
 * All rights reserved
 *
 * Distribute freely, except: don't remove my name from the source or
 * documentation (don't take credit for my work), mark your changes (don't
 * get me blamed for your possible bugs), don't alter or remove this
 * notice.  May be sold if buildable source is provided to buyer.  No
 * warrantee of any kind, express or implied, is included with this
 * software; use at your own risk, responsibility for damages (if any) to
 * anyone resulting from the use of this software rests entirely with the
 * user.
 *
 * Send bug reports, bug fixes, enhancements, requests, flames, etc., and
 * I'll try to keep a version up to date.  I can be reached as follows:
 * Paul Vixie          <paul@vix.com>          uunet!decwrl!vixie!paul
 */

/* vix 26jan87 [RCS has the rest of the log]
 * vix 30dec86 [written]
 */


#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <grp.h>
#ifdef WITH_AUDIT
#include <libaudit.h>
#endif
#if defined(SYSLOG)
# include <syslog.h>
#endif
#include "cron.h"


#if defined(LOG_DAEMON) && !defined(LOG_CRON)
#define LOG_CRON LOG_DAEMON
#endif


int
strcmp_until(left, right, until)
	char	*left;
	char	*right;
	int	until;
{
	register int	diff;

	while (*left && *left != until && *left == *right) {
		left++;
		right++;
	}

	if ((*left=='\0' || *left == until) &&
	    (*right=='\0' || *right == until)) {
		diff = 0;
	} else {
		diff = *left - *right;
	}

	return diff;
}


/* strdtb(s) - delete trailing blanks in string 's' and return new length
 */
int
strdtb(s)
	char	*s;
{
	char	*x = s;

	/* scan forward to the null
	 */
	while (*x)
		x++;

	/* scan backward to either the first character before the string,
	 * or the last non-blank in the string, whichever comes first.
	 */
	do	{x--;}
	while (x >= s && isspace(*x));

	/* one character beyond where we stopped above is where the null
	 * goes.
	 */
	*++x = '\0';

	/* the difference between the position of the null character and
	 * the position of the first character of the string is the length.
	 */
	return x - s;
}


int
set_debug_flags(flags)
	char	*flags;
{
	/* debug flags are of the form    flag[,flag ...]
	 *
	 * if an error occurs, print a message to stdout and return FALSE.
	 * otherwise return TRUE after setting ERROR_FLAGS.
	 */

#if !DEBUGGING

	printf("this program was compiled without debugging enabled\n");
	return FALSE;

#else /* DEBUGGING */

	char	*pc = flags;

	DebugFlags = 0;

	while (*pc) {
		char	**test;
		int	mask;

		/* try to find debug flag name in our list.
		 */
		for (	test = DebugFlagNames, mask = 1;
			*test && strcmp_until(*test, pc, ',');
			test++, mask <<= 1
		    )
			;

		if (!*test) {
			fprintf(stderr,
				"unrecognized debug flag <%s> <%s>\n",
				flags, pc);
			return FALSE;
		}

		DebugFlags |= mask;

		/* skip to the next flag
		 */
		while (*pc && *pc != ',')
			pc++;
		if (*pc == ',')
			pc++;
	}

	if (DebugFlags) {
		int	flag;

		fprintf(stderr, "debug flags enabled:");

		for (flag = 0;  DebugFlagNames[flag];  flag++)
			if (DebugFlags & (1 << flag))
				fprintf(stderr, " %s", DebugFlagNames[flag]);
		fprintf(stderr, "\n");
	}

	return TRUE;

#endif /* DEBUGGING */
}


void
set_cron_uid()
{
#if defined(BSD) || defined(POSIX)
	if (seteuid(ROOT_UID) < OK) {
		perror("seteuid");
		exit(ERROR_EXIT);
	}
#else
	if (setuid(ROOT_UID) < OK) {
		perror("setuid");
		exit(ERROR_EXIT);
	}
#endif
}


void
set_cron_cwd()
{
	struct stat	sb;
	mode_t		um;
	struct group	*gr;
	
	/* first check for CRONDIR ("/var/cron" or some such)
	 */
	if (stat(CRONDIR, &sb) < OK && errno == ENOENT) {
		perror(CRONDIR);

		/* crontab(1) running SGID crontab shouldn't attempt to create
		 * directories */
		if (getuid() != 0 )
			exit(ERROR_EXIT);

		um = umask(000);
		if (OK == mkdir(CRONDIR, CRONDIR_MODE)) {
			fprintf(stderr, "%s: created\n", CRONDIR);
			stat(CRONDIR, &sb);
		} else {
			fprintf(stderr, "%s: mkdir: %s\n", CRONDIR,
				strerror(errno));
			exit(ERROR_EXIT);
		}
		(void) umask(um);
	}
	if (!(sb.st_mode & S_IFDIR)) {
		fprintf(stderr, "'%s' is not a directory, bailing out.\n",
			CRONDIR);
		exit(ERROR_EXIT);
	}
	if (chdir(CRONDIR) < OK) {
		fprintf(stderr, "%s: chdir: %s\n", CRONDIR, strerror(errno));
		exit(ERROR_EXIT);
	}

	/* CRONDIR okay (now==CWD), now look at SPOOL_DIR ("tabs" or some such)
	 */
	if (stat(SPOOL_DIR, &sb) < OK && errno == ENOENT) {
		perror(SPOOL_DIR);

		/* crontab(1) running SGID crontab shouldn't attempt to create
		 * directories */
		if (getuid() != 0 )
			exit(ERROR_EXIT);

		um = umask(000);
		if (OK == mkdir(SPOOL_DIR, SPOOL_DIR_MODE)) {
			fprintf(stderr, "%s: created\n", SPOOL_DIR);
		} else {
			fprintf(stderr, "%s: mkdir: %s\n", SPOOL_DIR,
				strerror(errno));
			exit(ERROR_EXIT);
		}
		(void) umask(um);

		if (!(gr = getgrnam(SPOOL_DIR_GROUP))) {
			fprintf(stderr, "%s: getgrnam: %s\n", SPOOL_DIR,
				strerror(errno));
			exit(ERROR_EXIT);
		}
		if (OK == chown(SPOOL_DIR, -1, gr->gr_gid)) {
			fprintf(stderr, "%s: chowned\n", SPOOL_DIR);
				stat(SPOOL_DIR, &sb);
		} else {
			fprintf(stderr, "%s: chown: %s\n", SPOOL_DIR,
			strerror(errno));
			exit(ERROR_EXIT);
		}
	}
	if (!(sb.st_mode & S_IFDIR)) {
		fprintf(stderr, "'%s' is not a directory, bailing out.\n",
			SPOOL_DIR);
		exit(ERROR_EXIT);
	}
}


/* acquire_daemonlock() - write our PID into /etc/crond.pid, unless
 *	another daemon is already running, which we detect here.
 *
 * note: main() calls us twice; once before forking, once after.
 *	we maintain static storage of the file pointer so that we
 *	can rewrite our PID into the PIDFILE after the fork.
 *
 * it would be great if fflush() disassociated the file buffer.
 */
void
acquire_daemonlock(closeflag)
	int closeflag;
{
	static	FILE	*fp = NULL;

	if (closeflag && fp) {
		fclose(fp);
		fp = NULL;
		return;
	}

	if (!fp) {
		char	pidfile[MAX_FNAME];
		char	buf[MAX_TEMPSTR];
		int	fd, otherpid;

		(void) snprintf(pidfile, MAX_FNAME, PIDFILE, PIDDIR);
		if ((-1 == (fd = open(pidfile, O_RDWR|O_CREAT, 0644)))
		    || (NULL == (fp = fdopen(fd, "r+")))
		    ) {
			snprintf(buf, MAX_TEMPSTR, "can't open or create %s: %s",
				pidfile, strerror(errno));
			fprintf(stderr, "%s: %s\n", ProgramName, buf);
			log_it("CRON", getpid(), "DEATH", buf);
			exit(ERROR_EXIT);
		}

		if (flock(fd, LOCK_EX|LOCK_NB) < OK) {
			int save_errno = errno;

			fscanf(fp, "%d", &otherpid);
			snprintf(buf, MAX_TEMPSTR, "can't lock %s, otherpid may be %d: %s",
				pidfile, otherpid, strerror(save_errno));
			fprintf(stderr, "%s: %s\n", ProgramName, buf);
			log_it("CRON", getpid(), "DEATH", buf);
			exit(ERROR_EXIT);
		}
		snprintf(buf, MAX_TEMPSTR, "pidfile fd = %d", fd);
		log_it("CRON", getpid(), "INFO", buf);
		(void) fcntl(fd, F_SETFD, 1);
	}

	rewind(fp);
	fprintf(fp, "%d\n", getpid());
	fflush(fp);
	(void) ftruncate(fileno(fp), ftell(fp));

	/* abandon fd and fp even though the file is open. we need to
	 * keep it open and locked, but we don't need the handles elsewhere.
	 */
	
}

/* get_char(file) : like getc() but increment LineNumber on newlines
 */
int
get_char(file)
	FILE	*file;
{
	int	ch;

	/*
	 * Sneaky hack: we wrapped an in-memory buffer into a FILE*
	 * to minimize changes to cron.c.
	 *
	 * This code replaces:
	 * ch = getc(file);
	 */
	file_buffer *buffer = (file_buffer *) file;

	if (buffer->unget_count > 0)
	{
		ch = buffer->unget_data[--buffer->unget_count];
	}
	else if (buffer->pointer == buffer->length)
	{
		ch = '\0';
	}
	else
	{
		ch = buffer->data[buffer->pointer++];
	}

	if (ch == '\n')
		Set_LineNum(LineNumber + 1);
	return ch;
}


/* unget_char(ch, file) : like ungetc but do LineNumber processing
 */
void
unget_char(ch, file)
	int	ch;
	FILE	*file;
{

	/*
	 * Sneaky hack: we wrapped an in-memory buffer into a FILE*
	 * to minimize changes to cron.c.
	 *
	 * This code replaces:
	 * ungetc(ch, file);
	 */
	file_buffer *buffer = (file_buffer *) file;

	if (buffer->unget_count >= 1024)
	{	
		perror("ungetc limit exceeded");
		exit(ERROR_EXIT);
	}

	buffer->unget_data[buffer->unget_count++] = ch;

	if (ch == '\n')
	       Set_LineNum(LineNumber - 1);
}


/* get_string(str, max, file, termstr) : like fgets() but
 *		(1) has terminator string which should include \n
 *		(2) will always leave room for the null
 *		(3) uses get_char() so LineNumber will be accurate
 *		(4) returns EOF or terminating character, whichever
 */
int
get_string(string, size, file, terms)
	char	*string;
	int	size;
	FILE	*file;
	char	*terms;
{
	int	ch;

	while (EOF != (ch = get_char(file)) && !strchr(terms, ch)) {
		if (size > 1) {
			*string++ = (char) ch;
			size--;
		}
	}

	if (size > 0)
		*string = '\0';

	return ch;
}


/* skip_comments(file) : read past comment (if any)
 */
void
skip_comments(file)
	FILE	*file;
{
	int	ch;

	while (EOF != (ch = get_char(file))) {
		/* ch is now the first character of a line.
		 */

		while (ch == ' ' || ch == '\t')
			ch = get_char(file);

		if (ch == EOF)
			break;

		/* ch is now the first non-blank character of a line.
		 */

		if (ch != '\n' && ch != '#')
			break;

		/* ch must be a newline or comment as first non-blank
		 * character on a line.
		 */

		while (ch != '\n' && ch != EOF)
			ch = get_char(file);

		/* ch is now the newline of a line which we're going to
		 * ignore.
		 */
	}
	if (ch != EOF)
		unget_char(ch, file);
}


/* int in_file(char *string, FILE *file)
 *	return TRUE if one of the lines in file matches string exactly,
 *	FALSE otherwise.
 */
static int
in_file(string, file)
	char *string;
	FILE *file;
{
	char line[MAX_TEMPSTR];

	rewind(file);
	while (fgets(line, MAX_TEMPSTR, file)) {
		if (line[0] != '\0')
			line[strlen(line)-1] = '\0';
		if (0 == strcmp(line, string))
			return TRUE;
	}
	return FALSE;
}


/* int allowed(char *username)
 *	returns TRUE if (ALLOW_FILE exists and user is listed)
 *	or (DENY_FILE exists and user is NOT listed)
 *	or (neither file exists but user=="root" so it's okay)
 */
int
allowed(username)
	char *username;
{
	static int	init = FALSE;
	static FILE	*allow, *deny;
	int     isallowed;

        /* Root cannot be denied execution of cron jobs even if in the
	 * 'DENY_FILE' so we return inmediately */
        if (strcmp(username, ROOT_USER) == 0)
                return (TRUE);

	isallowed = FALSE;
#if defined(ALLOW_ONLY_ROOT)
	Debug(DMISC, "only root access is allowed")
#else
	if (!init) {
		init = TRUE;
#if defined(ALLOW_FILE) && defined(DENY_FILE)
		allow = fopen(ALLOW_FILE, "r");
		deny = fopen(DENY_FILE, "r");
		Debug(DMISC, ("allow/deny enabled, %d/%d\n", !!allow, !!deny))
#else
		allow = NULL;
		deny = NULL;
#endif
	}

	if (allow) 
		isallowed = in_file(username, allow);
	else
		isallowed = TRUE; /* Allow access if ALLOW_FILE does not exist */
	if (deny && !allow)
		isallowed = !in_file(username, deny);
#endif

#ifdef WITH_AUDIT
       /* Log an audit message if the user is rejected */ 
       if (isallowed == FALSE) {
               int audit_fd = audit_open();
               audit_log_user_message(audit_fd, AUDIT_USER_START, "cron deny",
                       NULL, NULL, NULL, 0);
               close(audit_fd);
       }
#endif
	return isallowed;
}


void
log_it(username, xpid, event, detail)
	char	*username;
	int	xpid;
	char	*event;
	char	*detail;
{
#if defined(LOG_FILE)
	PID_T			pid = xpid;
	char			*msg;
	TIME_T			now = time((TIME_T) 0);
	register struct tm	*t = localtime(&now);
	int 			msg_size;
#endif /*LOG_FILE*/


#if defined(LOG_FILE)
	/* we assume that MAX_TEMPSTR will hold the date, time, &punctuation.
	 */
	msg_size = strlen(username) + strlen(event) + strlen(detail) + MAX_TEMPSTR;
	msg = malloc(msg_size);
	if (msg == NULL) {
	    /* damn, out of mem and we did not test that before... */
	    fprintf(stderr, "%s: Run OUT OF MEMORY while %s\n",
		    ProgramName, __FUNCTION__);
	    return;
	}
	if (LogFD < OK) {
		LogFD = open(LOG_FILE, O_WRONLY|O_APPEND|O_CREAT, 0600);
		if (LogFD < OK) {
			fprintf(stderr, "%s: %s: open: %s\n",
				ProgramName, LOG_FILE, strerror(errno));
		} else {
			(void) fcntl(LogFD, F_SETFD, 1);
		}
	}

	/* we have to snprintf() it because fprintf() doesn't always write
	 * everything out in one chunk and this has to be atomically appended
	 * to the log file.
	 */
	snprintf(msg, msg_size, "%s (%02d/%02d-%02d:%02d:%02d-%d) %s (%s)\n",
		username,
		t->tm_mon+1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec, pid,
		event, detail);

	/* we have to run strlen() because snprintf() returns (char*) on old BSD
	 */
	if (LogFD < OK || write(LogFD, msg, strlen(msg)) < OK) {
		if (LogFD >= OK)
			perror(LOG_FILE);
		fprintf(stderr, "%s: can't write to log file\n", ProgramName);
		write(STDERR, msg, strlen(msg));
	}

	free(msg);
#endif /*LOG_FILE*/

#if defined(SYSLOG)


	    /* we don't use LOG_PID since the pid passed to us by
	     * our client may not be our own.  therefore we want to
	     * print the pid ourselves.
	     */
	    /* SteveG says: That comment is not consistent with the
	       code, and makes no sense -- I suspect it's a remnant
	       of a cut-n-paster... */
# ifdef LOG_CRON
	openlog(ProgramName, LOG_PID, LOG_CRON);
# else
	openlog(ProgramName, LOG_PID);
# endif
	  
	syslog(LOG_INFO, "(%s) %s (%s)", username, event, detail);

	closelog();
#endif /*SYSLOG*/

#if DEBUGGING
	if (DebugFlags) {
		fprintf(stderr, "log_it: (%s %d) %s (%s)\n",
			username, xpid, event, detail);
	}
#endif
}


void
log_close() {
#if defined(LOG_FILE)
	if (LogFD != ERR) {
		close(LogFD);
		LogFD = ERR;
	}
#endif
#if defined(SYSLOG)
	closelog();
#endif
}


/* two warnings:
 *	(1) this routine is fairly slow
 *	(2) it returns a pointer to static storage
 */
char *
first_word(s, t)
	register char *s;	/* string we want the first word of */
	register char *t;	/* terminators, implicitly including \0 */
{
	static char retbuf[2][MAX_TEMPSTR + 1];	/* sure wish C had GC */
	static int retsel = 0;
	register char *rb, *rp;

	/* select a return buffer */
	retsel = 1-retsel;
	rb = &retbuf[retsel][0];
	rp = rb;

	/* skip any leading terminators */
	while (*s && (NULL != strchr(t, *s))) {
		s++;
	}

	/* copy until next terminator or full buffer */
	while (*s && (NULL == strchr(t, *s)) && (rp < &rb[MAX_TEMPSTR])) {
		*rp++ = *s++;
	}

	/* finish the return-string and return it */
	*rp = '\0';
	return rb;
}


/* warning:
 *	heavily ascii-dependent.
 */
static void
mkprint(dst, src, len)
	register char *dst;
	register unsigned char *src;
	register int len;
{
	while (len-- > 0)
	{
		register unsigned char ch = *src++;

		if (ch < ' ') {			/* control character */
			*dst++ = '^';
			*dst++ = ch + '@';
		} else if (ch < 0177) {		/* printable */
			*dst++ = ch;
		} else if (ch == 0177) {	/* delete/rubout */
			*dst++ = '^';
			*dst++ = '?';
		} else {			/* parity character */
		    /* well, the following snprintf is paranoid, but that will
		     * keep grep happy */
		    snprintf(dst, 5, "\\%03o", ch);
		    dst += 4;
		}
	}
	*dst = '\0';
}


/* warning:
 *	returns a pointer to malloc'd storage, you must call free yourself.
 */
char *
mkprints(src, len)
	register unsigned char *src;
	register unsigned int len;
{
	register char *dst = malloc(len*4 + 1);

	if (dst)
		mkprint(dst, src, len);

	return dst;
}


#ifdef MAIL_DATE
/* Sat, 27 Feb 1993 11:44:51 -0800 (CST)
 * 1234567890123456789012345678901234567
 */
char *
arpadate(clock)
	time_t *clock;
{
	static char ret[64];	/* zone name might be >3 chars */
	time_t t = clock ? *clock : time(NULL);
	struct tm *tm = localtime(&t);
	char *qmark;
	size_t len;
        long gmtoff = get_gmtoff(&t, tm);
        int hours = gmtoff / 3600;
        int minutes = (gmtoff - (hours * 3600)) / 60;

	if (minutes < 0)
		minutes = -minutes;

	/* Defensive coding (almost) never hurts... */
	len = strftime(ret, sizeof(ret), "%a, %e %b %Y %T ????? (%Z)", tm);
	if (len == 0) {
		ret[0] = '?';
		ret[1] = '\0';
		return ret;
	}
	qmark = strchr(ret, '?');
	if (qmark && len - (qmark - ret) >= 6) {
		snprintf(qmark, 6, "% .2d%.2d", hours, minutes);
		qmark[5] = ' ';
	}
	return ret;
}
#endif /*MAIL_DATE*/


#ifdef HAVE_SAVED_UIDS
static uid_t save_euid, save_egid;
int swap_uids()
{
	save_euid = geteuid(); save_egid = getegid();
	return (setegid(getgid()) || seteuid(getuid())) ? -1 : 0;
}
int swap_uids_back()
{
	return (setegid(save_egid) || seteuid(save_euid)) ? -1 : 0;
}
#else /*HAVE_SAVED_UIDS*/
int swap_uids()
{
	return (setregid(getegid(), getgid()) || setreuid(geteuid(), getuid()))
		? -1 : 0;
}
int swap_uids_back() { return swap_uids(); }
#endif /*HAVE_SAVED_UIDS*/


/* Return the offset from GMT in seconds (algorithm taken from sendmail).
 *
 * warning:
 *	clobbers the static storage space used by localtime() and gmtime().
 *	If the local pointer is non-NULL it *must* point to a local copy.
 */
#ifndef HAVE_TM_GMTOFF
long get_gmtoff(time_t *clock, struct tm *local)
{
	struct tm gmt;
	long offset;

	gmt = *gmtime(clock);
	if (local == NULL)
		local = localtime(clock);

	offset = (local->tm_sec - gmt.tm_sec) +
	    ((local->tm_min - gmt.tm_min) * 60) +
	    ((local->tm_hour - gmt.tm_hour) * 3600);

	/* Timezone may cause year rollover to happen on a different day. */
	if (local->tm_year < gmt.tm_year)
		offset -= 24 * 3600;
	else if (local->tm_year > gmt.tm_year)
		offset += 24 * 3600;
	else if (local->tm_yday < gmt.tm_yday)
		offset -= 24 * 3600;
	else if (local->tm_yday > gmt.tm_yday)
		offset += 24 * 3600;

	return (offset);
}
#endif /* HAVE_TM_GMTOFF */

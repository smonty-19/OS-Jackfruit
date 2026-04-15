    /*
    * engine.c - Supervised Multi-Container Runtime (User Space)
    *
    * Intentionally partial starter:
    *   - command-line shape is defined
    *   - key runtime data structures are defined
    *   - bounded-buffer skeleton is defined
    *   - supervisor / client split is outlined
    *
    * Students are expected to design:
    *   - the control-plane IPC implementation
    *   - container lifecycle and metadata synchronization
    *   - clone + namespace setup for each container
    *   - producer/consumer behavior for log buffering
    *   - signal handling and graceful shutdown
    */

    #define _GNU_SOURCE
    #include <errno.h>
    #include <fcntl.h>
    #include <limits.h>
    #include <pthread.h>
    #include <sched.h>
    #include <signal.h>
    #include <stdio.h>
    #include <stdlib.h>
    #include <string.h>
    #include <sys/ioctl.h>
    #include <sys/mount.h>
    #include <sys/socket.h>
    #include <sys/stat.h>
    #include <sys/types.h>
    #include <sys/un.h>
    #include <sys/wait.h>
    #include <time.h>
    #include <unistd.h>
    #include <sys/resource.h>
    #include <sys/time.h>
    #include <sys/select.h>

    #include "monitor_ioctl.h"

    #define STACK_SIZE (1024 * 1024)
    #define CONTAINER_ID_LEN 32
    #define CONTROL_PATH "/tmp/mini_runtime.sock"
    #define LOG_DIR "/tmp/engine-logs"
    #define CONTROL_MESSAGE_LEN 256
    #define CHILD_COMMAND_LEN 256
    #define LOG_CHUNK_SIZE 4096
    #define LOG_BUFFER_CAPACITY 16
    #define DEFAULT_SOFT_LIMIT (40UL << 20)
    #define DEFAULT_HARD_LIMIT (64UL << 20)

    typedef enum {
        CMD_SUPERVISOR = 0,
        CMD_START,
        CMD_RUN,
        CMD_PS,
        CMD_LOGS,
        CMD_STOP
    } command_kind_t;

    typedef enum {
        CONTAINER_STARTING = 0,
        CONTAINER_RUNNING,
        CONTAINER_STOPPED,
        CONTAINER_KILLED,
        CONTAINER_EXITED
    } container_state_t;

    typedef struct container_record {
        char id[CONTAINER_ID_LEN];
        pid_t host_pid;
        time_t started_at;
        container_state_t state;
        unsigned long soft_limit_bytes;
        unsigned long hard_limit_bytes;
        int exit_code;
        int exit_signal;
        int stop_requested;     // set when stop <id> is requested
        int hard_limit_killed;  // set when SIGKILL and stop_requested == 0
        char log_path[PATH_MAX];
        pthread_t producer_thread;
        int log_read_fd;
        int producer_started;
        struct container_record *next;
    } container_record_t;

    typedef struct {
        char container_id[CONTAINER_ID_LEN];
        size_t length;
        char data[LOG_CHUNK_SIZE];
    } log_item_t;

    typedef struct {
        log_item_t items[LOG_BUFFER_CAPACITY];
        size_t head;
        size_t tail;
        size_t count;
        int shutting_down;
        pthread_mutex_t mutex;
        pthread_cond_t not_empty;
        pthread_cond_t not_full;
    } bounded_buffer_t;

    typedef struct {
        command_kind_t kind;
        char container_id[CONTAINER_ID_LEN];
        char rootfs[PATH_MAX];
        char command[CHILD_COMMAND_LEN];
        unsigned long soft_limit_bytes;
        unsigned long hard_limit_bytes;
        int nice_value;
    } control_request_t;

    typedef struct {
        int status;
        char message[CONTROL_MESSAGE_LEN];
    } control_response_t;

    typedef struct {
        char id[CONTAINER_ID_LEN];
        char rootfs[PATH_MAX];
        char command[CHILD_COMMAND_LEN];
        int nice_value;
        int log_write_fd;
    } child_config_t;

    typedef struct {
        int server_fd;
        int monitor_fd;
        int should_stop;
        pthread_t logger_thread;
        bounded_buffer_t log_buffer;
        pthread_mutex_t metadata_lock;
        container_record_t *containers;
    } supervisor_ctx_t;

    typedef struct {
        supervisor_ctx_t *ctx;
        char container_id[CONTAINER_ID_LEN];
        int read_fd;
    } producer_cfg_t;

    static void usage(const char *prog)
    {
        fprintf(stderr,
                "Usage:\n"
                "  %s supervisor <base-rootfs>\n"
                "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
                "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
                "  %s ps\n"
                "  %s logs <id>\n"
                "  %s stop <id>\n",
                prog, prog, prog, prog, prog, prog);
    }

    static int parse_mib_flag(const char *flag,
                            const char *value,
                            unsigned long *target_bytes)
    {
        char *end = NULL;
        unsigned long mib;

        errno = 0;
        mib = strtoul(value, &end, 10);
        if (errno != 0 || end == value || *end != '\0') {
            fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
            return -1;
        }

        if (mib > ULONG_MAX / (1UL << 20)) {
            fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
            return -1;
        }

        *target_bytes = mib * (1UL << 20);
        return 0;
    }

    static int parse_optional_flags(control_request_t *req,
                                    int argc,
                                    char *argv[],
                                    int start_index)
    {
        int i;

        for (i = start_index; i < argc; i += 2) {
            char *end = NULL;
            long nice_value;

            if (i + 1 >= argc) {
                fprintf(stderr, "Missing value for option: %s\n", argv[i]);
                return -1;
            }

            if (strcmp(argv[i], "--soft-mib") == 0) {
                if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                    return -1;
                continue;
            }

            if (strcmp(argv[i], "--hard-mib") == 0) {
                if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                    return -1;
                continue;
            }

            if (strcmp(argv[i], "--nice") == 0) {
                errno = 0;
                nice_value = strtol(argv[i + 1], &end, 10);
                if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                    nice_value < -20 || nice_value > 19) {
                    fprintf(stderr,
                            "Invalid value for --nice (expected -20..19): %s\n",
                            argv[i + 1]);
                    return -1;
                }
                req->nice_value = (int)nice_value;
                continue;
            }

            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return -1;
        }

        if (req->soft_limit_bytes > req->hard_limit_bytes) {
            fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
            return -1;
        }

        return 0;
    }

    static const char *state_to_string(container_state_t state)
    {
        switch (state) {
        case CONTAINER_STARTING:
            return "starting";
        case CONTAINER_RUNNING:
            return "running";
        case CONTAINER_STOPPED:
            return "stopped";
        case CONTAINER_KILLED:
            return "killed";
        case CONTAINER_EXITED:
            return "exited";
        default:
            return "unknown";
        }
    }

    static int bounded_buffer_init(bounded_buffer_t *buffer)
    {
        int rc;

        memset(buffer, 0, sizeof(*buffer));

        rc = pthread_mutex_init(&buffer->mutex, NULL);
        if (rc != 0)
            return rc;

        rc = pthread_cond_init(&buffer->not_empty, NULL);
        if (rc != 0) {
            pthread_mutex_destroy(&buffer->mutex);
            return rc;
        }

        rc = pthread_cond_init(&buffer->not_full, NULL);
        if (rc != 0) {
            pthread_cond_destroy(&buffer->not_empty);
            pthread_mutex_destroy(&buffer->mutex);
            return rc;
        }

        return 0;
    }

    static void bounded_buffer_destroy(bounded_buffer_t *buffer)
    {
        pthread_cond_destroy(&buffer->not_full);
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
    }

    static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
    {
        pthread_mutex_lock(&buffer->mutex);
        buffer->shutting_down = 1;
        pthread_cond_broadcast(&buffer->not_empty);
        pthread_cond_broadcast(&buffer->not_full);
        pthread_mutex_unlock(&buffer->mutex);
    }

    /*
    * TODO:
    * Implement producer-side insertion into the bounded buffer.
    *
    * Requirements:
    *   - block or fail according to your chosen policy when the buffer is full
    *   - wake consumers correctly
    *   - stop cleanly if shutdown begins
    */
    int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
    {
        pthread_mutex_lock(&buffer->mutex);

        while (!buffer->shutting_down && buffer->count == LOG_BUFFER_CAPACITY) {
            pthread_cond_wait(&buffer->not_full, &buffer->mutex);
        }

        if (buffer->shutting_down) {
            pthread_mutex_unlock(&buffer->mutex);
            return 1; // indicate shutdown
        }

        buffer->items[buffer->tail] = *item;
        buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
        buffer->count++;

        pthread_cond_signal(&buffer->not_empty);
        pthread_mutex_unlock(&buffer->mutex);
        return 0;
    }

    /*
    * TODO:
    * Implement consumer-side removal from the bounded buffer.
    *
    * Requirements:
    *   - wait correctly while the buffer is empty
    *   - return a useful status when shutdown is in progress
    *   - avoid races with producers and shutdown
    */
    int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
    {
        pthread_mutex_lock(&buffer->mutex);

        while (!buffer->shutting_down && buffer->count == 0) {
            pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
        }

        if (buffer->count == 0 && buffer->shutting_down) {
            pthread_mutex_unlock(&buffer->mutex);
            return 1; // drained + shutting down
        }

        *item = buffer->items[buffer->head];
        buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
        buffer->count--;

        pthread_cond_signal(&buffer->not_full);
        pthread_mutex_unlock(&buffer->mutex);
        return 0;
    }

    /*
    * TODO:
    * Implement the logging consumer thread.
    *
    * Suggested responsibilities:
    *   - remove log chunks from the bounded buffer
    *   - route each chunk to the correct per-container log file
    *   - exit cleanly when shutdown begins and pending work is drained
    */
    void *logging_thread(void *arg)
    {
        supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;

        while (1) {
            log_item_t item;
            int rc = bounded_buffer_pop(&ctx->log_buffer, &item);
            if (rc == 1) // shutdown + drained
                break;

            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

            int fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
            if (fd < 0) {
                // can't log; drop chunk (rare)
                continue;
            }

            (void)write(fd, item.data, item.length);
            close(fd);
        }

        return NULL;
    }

    static void *producer_thread(void *arg)
    {
        producer_cfg_t *pc = (producer_cfg_t *)arg;

        while (1) {
            log_item_t item;
            memset(&item, 0, sizeof(item));
            strncpy(item.container_id, pc->container_id, sizeof(item.container_id) - 1);

            ssize_t n = read(pc->read_fd, item.data, sizeof(item.data));
            if (n == 0) break;              // EOF
            if (n < 0) {
                if (errno == EINTR) continue;
                break;
            }

            item.length = (size_t)n;
            if (bounded_buffer_push(&pc->ctx->log_buffer, &item) != 0) {
                break; // shutdown
            }
        }

        close(pc->read_fd);
        free(pc);
        return NULL;
    }

    static volatile sig_atomic_t g_stop = 0;
    static volatile sig_atomic_t g_run_interrupt = 0;

    static void handle_stop(int sig)
    {
        (void)sig;
        g_stop = 1;
    }
    static void handle_run_client_sig(int sig)
    {
        (void)sig;
        g_run_interrupt = 1;
    }

    static int ensure_dir(const char *path, mode_t mode)
    {
        struct stat st;
        if (stat(path, &st) == 0) return 0;
        if (mkdir(path, mode) < 0) return -1;
        return 0;
    }

    /*
    * TODO:
    * Implement the clone child entrypoint.
    *
    * Required outcomes:
    *   - isolated PID / UTS / mount context
    *   - chroot or pivot_root into rootfs
    *   - working /proc inside container
    *   - stdout / stderr redirected to the supervisor logging path
    *   - configured command executed inside the container
    */
    int child_fn(void *arg)
    {
        child_config_t *cfg = (child_config_t *)arg;

        // Redirect stdout/stderr to supervisor (pipe write end)
        if (cfg->log_write_fd >= 0) {
            dup2(cfg->log_write_fd, STDOUT_FILENO);
            dup2(cfg->log_write_fd, STDERR_FILENO);
            close(cfg->log_write_fd);
        }

        // UTS namespace demo: hostname = container id
        if (sethostname(cfg->id, strnlen(cfg->id, CONTAINER_ID_LEN)) < 0) {
            perror("sethostname");
            return 1;
        }

        // Prevent mount propagation to host
        if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
            perror("mount MS_PRIVATE");
            return 1;
        }

        // chroot into the container rootfs
        if (chroot(cfg->rootfs) < 0) {
            perror("chroot");
            return 1;
        }
        if (chdir("/") < 0) {
            perror("chdir");
            return 1;
        }

        // Mount /proc inside container
        if (ensure_dir("/proc", 0555) < 0 && errno != EEXIST) {
            perror("mkdir /proc");
            return 1;
        }
        if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
            perror("mount proc");
            return 1;
        }

        if (cfg->nice_value != 0) {
            if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0) {
                perror("setpriority");
                // not fatal
            }
        } 

        // Exec single command path for Task 1 (Task 2 will add argv parsing)
        char *const child_argv[] = { cfg->command, NULL };
        execv(cfg->command, child_argv);

        perror("execv");
        return 1;
    }


    int register_with_monitor(int monitor_fd,
                            const char *container_id,
                            pid_t host_pid,
                            unsigned long soft_limit_bytes,
                            unsigned long hard_limit_bytes)
    {
        struct monitor_request req;

        if (monitor_fd < 0)
            return 0; // best-effort: monitor not available

        memset(&req, 0, sizeof(req));
        req.pid = host_pid;
        req.soft_limit_bytes = soft_limit_bytes;
        req.hard_limit_bytes = hard_limit_bytes;
        strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

        if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) {
            perror("ioctl(MONITOR_REGISTER)");
            return -1;
        }

        return 0;
    }

    int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
    {
        struct monitor_request req;

        if (monitor_fd < 0)
            return 0; // best-effort

        memset(&req, 0, sizeof(req));
        req.pid = host_pid;
        strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

        if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0) {
            // Not fatal (container may already be removed by timer or never registered)
            // perror("ioctl(MONITOR_UNREGISTER)");
            return -1;
        }

        return 0;
    }

    static container_record_t *record_container(supervisor_ctx_t *ctx,const char *id,pid_t host_pid,const char *log_path,unsigned long soft_limit,unsigned long hard_limit)
    {
        container_record_t *rec = calloc(1, sizeof(*rec));
        if (!rec)
            return NULL;

        strncpy(rec->id, id, sizeof(rec->id) - 1);
        rec->host_pid = host_pid;
        rec->started_at = time(NULL);
        rec->state = CONTAINER_RUNNING;
        rec->soft_limit_bytes = soft_limit;
        rec->hard_limit_bytes = hard_limit;
        rec->exit_code = -1;
        rec->exit_signal = 0;
        strncpy(rec->log_path, log_path, sizeof(rec->log_path) - 1);

        pthread_mutex_lock(&ctx->metadata_lock);
        rec->next = ctx->containers;
        ctx->containers = rec;
        pthread_mutex_unlock(&ctx->metadata_lock);

        return rec;
    }

    static void reap_children(supervisor_ctx_t *ctx)
    {
        int status;
        pid_t pid;

        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
            pthread_t to_join;
            int join_needed = 0;

            /* --- NEW: capture info for monitor unregister outside the lock --- */
            char cid[CONTAINER_ID_LEN];
            int have_cid = 0;

            pthread_mutex_lock(&ctx->metadata_lock);

            for (container_record_t *c = ctx->containers; c; c = c->next) {
                if (c->host_pid != pid)
                    continue;

                /* --- NEW: copy container id for unregister --- */
                strncpy(cid, c->id, sizeof(cid) - 1);
                cid[sizeof(cid) - 1] = '\0';
                have_cid = 1;

                // Update container state first
                if (WIFEXITED(status)) {
                    c->state = CONTAINER_EXITED;
                    c->exit_code = WEXITSTATUS(status);
                    c->exit_signal = 0;
                } else if (WIFSIGNALED(status)) {
                    int sig = WTERMSIG(status);
                    c->state = CONTAINER_KILLED;
                    c->exit_code = -1;
                    c->exit_signal = WTERMSIG(status);
                    if (sig == SIGKILL && c->stop_requested == 0) {
                        c->hard_limit_killed = 1;
                    }
                }

                // Capture producer thread to join after releasing lock
                if (c->producer_started) {
                    to_join = c->producer_thread;
                    c->producer_started = 0; // prevent double-join
                    join_needed = 1;
                }
                break;
            }

            pthread_mutex_unlock(&ctx->metadata_lock);

            /* --- NEW: unregister from monitor after unlock (best effort) --- */
            if (have_cid) {
                (void)unregister_from_monitor(ctx->monitor_fd, cid, pid);
            }

            if (join_needed) {
                pthread_join(to_join, NULL);
            }
        }
    }

    static const char *reason_string(const container_record_t *c)
    {
        if (!c) return "unknown";
        if (c->hard_limit_killed) return "hard_limit_killed";
        if (c->stop_requested && (c->state == CONTAINER_KILLED || c->state == CONTAINER_EXITED))
            return "stopped";
        if (c->state == CONTAINER_EXITED) return "exited";
        if (c->state == CONTAINER_KILLED) return "killed";
        if (c->state == CONTAINER_RUNNING) return "running";
        return "unknown";
    }

    static void print_ps(supervisor_ctx_t *ctx)
    {
        pthread_mutex_lock(&ctx->metadata_lock);
        printf("ID\tPID\tSTATE\tREASON\tSTART\tEXIT\tSIG\tLOG\n");
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            printf("%s\t%d\t%s\t%s\t%ld\t%d\t%d\t%s\n",
                c->id, c->host_pid, state_to_string(c->state),
                reason_string(c),
                (long)c->started_at, c->exit_code, c->exit_signal, c->log_path);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    static int setup_control_socket(void)
    {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("socket");
            return -1;
        }

        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        unlink(CONTROL_PATH);

        if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("bind");
            close(fd);
            return -1;
        }

        if (listen(fd, 16) < 0) {
            perror("listen");
            close(fd);
            return -1;
        }

        return fd;
    }

    static ssize_t read_line(int fd, char *buf, size_t cap)
    {
        size_t i = 0;
        while (i + 1 < cap) {
            char c;
            ssize_t n = read(fd, &c, 1);
            if (n == 0)
                break;
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                return -1;
            }
            if (c == '\n')
                break;
            buf[i++] = c;
        }
        buf[i] = '\0';
        return (ssize_t)i;
    }

    static int tokenize(char *line, char *argv[], int max_args)
    {
        int argc = 0;
        char *save = NULL;
        for (char *tok = strtok_r(line, " \t", &save);
            tok && argc < max_args;
            tok = strtok_r(NULL, " \t", &save)) {
            argv[argc++] = tok;
        }
        return argc;
    }

    static void write_str(int fd, const char *s)
    {
        (void)write(fd, s, strlen(s));
    }

    static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
    {
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            if (strncmp(c->id, id, CONTAINER_ID_LEN) == 0)
                return c;
        }
        return NULL;
    }

    static void fd_print_ps(supervisor_ctx_t *ctx, int fd)
    {
        char line[512];

        pthread_mutex_lock(&ctx->metadata_lock);

        snprintf(line, sizeof(line), "ID\tPID\tSTATE\tREASON\tSTART\tEXIT\tSIG\tLOG\n");
        write_str(fd, line);

        for (container_record_t *c = ctx->containers; c; c = c->next) {
            snprintf(line, sizeof(line), "%s\t%d\t%s\t%s\t%ld\t%d\t%d\t%s\n",
                    c->id, c->host_pid, state_to_string(c->state), reason_string(c),
                    (long)c->started_at, c->exit_code, c->exit_signal, c->log_path);
            write_str(fd, line);
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    static int spawn_container(supervisor_ctx_t *ctx,const char *id,const char *container_rootfs,const char *command,unsigned long soft_limit,unsigned long hard_limit,int nice_value,int wait_for_exit,int *out_exit_code,int *out_exit_signal)
    {
        int pipefd[2];
        void *stack = NULL;
        child_config_t *cfg = NULL;

        if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
            perror("mkdir logs");
            return -1;
        }

        // Create pipe for stdout/stderr -> supervisor producer thread
        if (pipe(pipefd) < 0) {
            perror("pipe");
            return -1;
        }

        // Allocate stack for clone()
        stack = malloc(STACK_SIZE);
        if (!stack) {
            perror("malloc stack");
            close(pipefd[0]);
            close(pipefd[1]);
            return -1;
        }

        cfg = calloc(1, sizeof(*cfg));
        if (!cfg) {
            perror("calloc cfg");
            free(stack);
            close(pipefd[0]);
            close(pipefd[1]);
            return -1;
        }

        strncpy(cfg->id, id, sizeof(cfg->id) - 1);
        strncpy(cfg->rootfs, container_rootfs, sizeof(cfg->rootfs) - 1);
        strncpy(cfg->command, command, sizeof(cfg->command) - 1);
        cfg->nice_value = nice_value;
        cfg->log_write_fd = pipefd[1]; // child writes here

        int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
        pid_t pid = clone(child_fn, (char *)stack + STACK_SIZE, flags, cfg);
        if (pid < 0) {
            perror("clone");
            free(cfg);
            free(stack);
            close(pipefd[0]);
            close(pipefd[1]);
            return -1;
        }

        // Parent: close write end, keep read end for producer thread
        close(pipefd[1]);

        // Prepare log path (consumer thread writes here)
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, id);

        // Record metadata + start producer thread (joinable)
        pthread_mutex_lock(&ctx->metadata_lock);

        if (find_container_locked(ctx, id)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            fprintf(stderr, "duplicate container id: %s\n", id);
            close(pipefd[0]);
            return -1;
        }

        container_record_t *rec = calloc(1, sizeof(*rec));
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            close(pipefd[0]);
            return -1;
        }

        strncpy(rec->id, id, sizeof(rec->id) - 1);
        rec->host_pid = pid;
        rec->started_at = time(NULL);
        rec->state = CONTAINER_RUNNING;
        rec->soft_limit_bytes = soft_limit;
        rec->hard_limit_bytes = hard_limit;
        rec->exit_code = -1;
        rec->exit_signal = 0;
        rec->stop_requested = 0;
        rec->hard_limit_killed = 0;
        strncpy(rec->log_path, log_path, sizeof(rec->log_path) - 1);

        rec->log_read_fd = pipefd[0];
        rec->producer_started = 0;

        // Insert into list first so consumer can resolve id->log_path
        rec->next = ctx->containers;
        ctx->containers = rec;

        (void)register_with_monitor(ctx->monitor_fd, id, pid, soft_limit, hard_limit);

        // Create producer thread config
        producer_cfg_t *pc = calloc(1, sizeof(*pc));
        if (!pc) {
            // Can't start producer; close read end, but keep container alive
            close(pipefd[0]);
        } else {
            pc->ctx = ctx;
            strncpy(pc->container_id, id, sizeof(pc->container_id) - 1);
            pc->read_fd = pipefd[0];

            int trc = pthread_create(&rec->producer_thread, NULL, producer_thread, pc);
            if (trc != 0) {
                errno = trc;
                perror("pthread_create(producer)");
                close(pipefd[0]);
                free(pc);
            } else {
                rec->producer_started = 1;
            }
        }

        pthread_mutex_unlock(&ctx->metadata_lock);

        // For "run": wait for container exit here and return status
        if (wait_for_exit) {
            int st;
            if (waitpid(pid, &st, 0) < 0) {
                perror("waitpid(run)");
                return -1;
            }

            int ec = -1, sig = 0;
            if (WIFEXITED(st)) ec = WEXITSTATUS(st);
            if (WIFSIGNALED(st)) sig = WTERMSIG(st);

            // Update metadata
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *c = find_container_locked(ctx, id);
            if (c) {
                if (WIFEXITED(st)) {
                    c->state = CONTAINER_EXITED;
                    c->exit_code = ec;
                    c->exit_signal = 0;
                } else if (WIFSIGNALED(st)) {
                    c->state = CONTAINER_KILLED;
                    c->exit_code = -1;
                    c->exit_signal = sig;
                }
            }

            // Join producer now that child has exited (pipe should hit EOF)
            if (c && c->producer_started) {
                pthread_t t = c->producer_thread;
                c->producer_started = 0;
                pthread_mutex_unlock(&ctx->metadata_lock);
                pthread_join(t, NULL);
                pthread_mutex_lock(&ctx->metadata_lock);
            }

            pthread_mutex_unlock(&ctx->metadata_lock);

            if (out_exit_code) *out_exit_code = ec;
            if (out_exit_signal) *out_exit_signal = sig;
        }

        return 0;
    }

    static int request_ps_and_capture(char *out, size_t cap)
    {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) return -1;

        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(fd);
            return -1;
        }

        const char *msg = "ps\n";
        if (write(fd, msg, strlen(msg)) < 0) {
            close(fd);
            return -1;
        }

        size_t used = 0;
        while (used + 1 < cap) {
            ssize_t n = read(fd, out + used, cap - 1 - used);
            if (n == 0) break;
            if (n < 0) {
                if (errno == EINTR) continue;
                close(fd);
                return -1;
            }
            used += (size_t)n;
        }
        out[used] = '\0';
        close(fd);
        return 0;
    }

    /* Parse the state field from the ps output for a given id.
    * Returns 0 on success, -1 if not found.
    */
    static int ps_find_state(const char *ps_text, const char *id, char *state_out, size_t state_cap)
    {
        const char *p = ps_text;

        while (*p) {
            /* Extract one line */
            const char *line_start = p;
            const char *line_end = strchr(p, '\n');
            size_t len = line_end ? (size_t)(line_end - line_start) : strlen(line_start);

            /* Move p forward */
            p = line_end ? (line_end + 1) : (line_start + len);

            if (len == 0) continue;

            /* Skip header lines */
            if (len >= 2 && strncmp(line_start, "OK", 2) == 0) continue;
            if (len >= 2 && strncmp(line_start, "ID", 2) == 0) continue;

            /* Copy line into temp buffer */
            char tmp[1024];
            if (len >= sizeof(tmp)) len = sizeof(tmp) - 1;
            memcpy(tmp, line_start, len);
            tmp[len] = '\0';

            /* Expected format:
            * ID\tPID\tSTATE\tREASON\t...
            */
            char *save = NULL;
            char *tok_id = strtok_r(tmp, "\t ", &save);
            char *tok_pid = strtok_r(NULL, "\t ", &save);
            char *tok_state = strtok_r(NULL, "\t ", &save);

            (void)tok_pid;

            if (!tok_id || !tok_state) continue;
            if (strcmp(tok_id, id) != 0) continue;

            strncpy(state_out, tok_state, state_cap - 1);
            state_out[state_cap - 1] = '\0';
            return 0;
        }

        return -1;
    }

    static int is_terminal_state(const char *state)
    {
        return (strcmp(state, "exited") == 0 ||
                strcmp(state, "killed") == 0 ||
                strcmp(state, "stopped") == 0);
    }

    static int handle_one_request(supervisor_ctx_t *ctx, int client_fd)
    {
        char line[512];
        if (read_line(client_fd, line, sizeof(line)) <= 0) {
            write_str(client_fd, "ERR empty\n");
            return 0;
        }

        char *argvv[32];
        int argc = tokenize(line, argvv, 32);
        if (argc <= 0) {
            write_str(client_fd, "ERR empty\n");
            return 0;
        }

        if (strcmp(argvv[0], "ps") == 0) {
            write_str(client_fd, "OK\n");
            fd_print_ps(ctx, client_fd);
            return 0;
        }

        if (strcmp(argvv[0], "logs") == 0) {
            if (argc < 2) {
                write_str(client_fd, "ERR usage: logs <id>\n");
                return 0;
            }

            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *c = find_container_locked(ctx, argvv[1]);
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (!c) {
                write_str(client_fd, "ERR unknown id\n");
                return 0;
            }

            int lf = open(c->log_path, O_RDONLY);
            if (lf < 0) {
                write_str(client_fd, "ERR cannot open log\n");
                return 0;
            }

            write_str(client_fd, "OK\n");
            char buf[4096];
            ssize_t n;
            while ((n = read(lf, buf, sizeof(buf))) > 0)
                (void)write(client_fd, buf, (size_t)n);
            close(lf);
            return 0;
        }

        if (strcmp(argvv[0], "stop") == 0) {
        if (argc < 2) {
            write_str(client_fd, "ERR usage: stop <id>\n");
            return 0;
        }

        pid_t pid = -1;

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = find_container_locked(ctx, argvv[1]);
        if (c) {
            c->stop_requested = 1;
            pid = c->host_pid;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (pid < 0) {
            write_str(client_fd, "ERR unknown id\n");
            return 0;
        }

        /* Send TERM and return immediately (don’t block the supervisor). */
        (void)kill(pid, SIGTERM);

        write_str(client_fd, "OK stop sent\n");
        return 0;
        }

        int is_start = (strcmp(argvv[0], "start") == 0);
        int is_run = (strcmp(argvv[0], "run") == 0);
        if (!is_start && !is_run) {
            write_str(client_fd, "ERR unknown command\n");
            return 0;
        }

        if (argc < 4) {
            write_str(client_fd, "ERR usage: start/run <id> <rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n");
            return 0;
        }

        control_request_t req;
        memset(&req, 0, sizeof(req));
        req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
        req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
        req.nice_value = 0;

        // parse flags from position 4 onward (argvv[4..])
        if (argc > 4) {
            if (parse_optional_flags(&req, argc, argvv, 4) != 0) {
                write_str(client_fd, "ERR invalid flags\n");
                return 0;
            }
        }

        int exit_code = -1, exit_sig = 0;
        if (spawn_container(ctx,
                            argvv[1], argvv[2], argvv[3],
                            req.soft_limit_bytes, req.hard_limit_bytes,
                            req.nice_value,
                            0,
                            NULL, NULL) != 0) {
            write_str(client_fd, "ERR spawn failed\n");
            return 0;
        }

        if (is_start) {
            write_str(client_fd, "OK started\n");
        } else {
            char msg[128];
            if (exit_sig != 0)
                snprintf(msg, sizeof(msg), "OK signaled %d\n", exit_sig);
            else
                snprintf(msg, sizeof(msg), "OK exit %d\n", exit_code);
            write_str(client_fd, msg);
        }

        return 0;
    }

    /*
    * TODO:
    * Implement the long-running supervisor process.
    *
    * Suggested responsibilities:
    *   - create and bind the control-plane IPC endpoint
    *   - initialize shared metadata and the bounded buffer
    *   - start the logging thread
    *   - accept control requests and update container state
    *   - reap children and respond to signals
    */
    static int run_supervisor(const char *rootfs)
    {
        supervisor_ctx_t ctx;
        int rc;

        memset(&ctx, 0, sizeof(ctx));
        ctx.server_fd = -1;
        ctx.monitor_fd = -1;

        (void)rootfs;

        rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
        if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

        rc = bounded_buffer_init(&ctx.log_buffer);
        if (rc != 0) { errno = rc; perror("bounded_buffer_init"); pthread_mutex_destroy(&ctx.metadata_lock); return 1; }

        rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
        if (rc != 0) {
            errno = rc;
            perror("pthread_create(logger)");
            return 1;
        }

        ctx.server_fd = setup_control_socket();
        if (ctx.server_fd < 0)
            return 1;

        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = handle_stop;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);

        printf("Supervisor listening on %s\n", CONTROL_PATH);

        ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
        if (ctx.monitor_fd < 0) {
            perror("open /dev/container_monitor");
        } else {
            fprintf(stderr, "supervisor: opened /dev/container_monitor fd=%d\n", ctx.monitor_fd);
        }

        while (!g_stop) {
            reap_children(&ctx);
            
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(ctx.server_fd, &rfds);

            struct timeval tv;
            tv.tv_sec = 1;
            tv.tv_usec = 0;

            int ready = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
            if (ready < 0) {
                if (errno == EINTR)
                    continue;
                perror("select");
                break;
            }

            if (ready == 0) {
                // timeout: no client; loop again so we can keep reaping
                continue;
            }

            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno == EINTR)
                    continue;
                perror("accept");
                break;
            }

            handle_one_request(&ctx, client_fd);
            close(client_fd);
        }

        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 0;
    }

    /*
    * TODO:
    * Implement the client-side control request path.
    *
    * The CLI commands should use a second IPC mechanism distinct from the
    * logging pipe. A UNIX domain socket is the most direct option, but a
    * FIFO or shared memory design is also acceptable if justified.
    */
    static int send_control_request(const control_request_t *req)
    {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("socket");
            return 1;
        }

        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("connect");
            close(fd);
            return 1;
        }

        char line[512];
        switch (req->kind) {
        case CMD_PS:
            snprintf(line, sizeof(line), "ps\n");
            break;
        case CMD_LOGS:
            snprintf(line, sizeof(line), "logs %s\n", req->container_id);
            break;
        case CMD_STOP:
            snprintf(line, sizeof(line), "stop %s\n", req->container_id);
            break;
        case CMD_START:
        case CMD_RUN: {
            const char *cmd = (req->kind == CMD_START) ? "start" : "run";
            snprintf(line, sizeof(line),
                    "%s %s %s %s --soft-mib %lu --hard-mib %lu --nice %d\n",
                    cmd,
                    req->container_id,
                    req->rootfs,
                    req->command,
                    req->soft_limit_bytes >> 20,
                    req->hard_limit_bytes >> 20,
                    req->nice_value);
            break;
        }
        default:
            snprintf(line, sizeof(line), "ps\n");
            break;
        }

        if (write(fd, line, strlen(line)) < 0) {
            perror("write");
            close(fd);
            return 1;
        }

        char buf[4096];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0) {
            fwrite(buf, 1, (size_t)n, stdout);
        }

        close(fd);
        return 0;
    }

    static int cmd_start(int argc, char *argv[])
    {
        control_request_t req;

        if (argc < 5) {
            fprintf(stderr,
                    "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                    argv[0]);
            return 1;
        }

        memset(&req, 0, sizeof(req));
        req.kind = CMD_START;
        strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
        strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
        strncpy(req.command, argv[4], sizeof(req.command) - 1);
        req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
        req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

        if (parse_optional_flags(&req, argc, argv, 5) != 0)
            return 1;

        return send_control_request(&req);
    }

    static int send_stop_request_simple(const char *id)
    {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) return -1;

        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(fd);
            return -1;
        }

        char line[128];
        snprintf(line, sizeof(line), "stop %s\n", id);
        (void)write(fd, line, strlen(line));

        // read and ignore response (best-effort)
        char buf[256];
        while (read(fd, buf, sizeof(buf)) > 0) { }
        close(fd);
        return 0;
    }

    static int send_run_request_with_forwarded_stop(const control_request_t *req)
    {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("socket");
            return 1;
        }

        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("connect");
            close(fd);
            return 1;
        }

        // Send "run ..." exactly like send_control_request does
        char line[512];
        snprintf(line, sizeof(line),
                "run %s %s %s --soft-mib %lu --hard-mib %lu --nice %d\n",
                req->container_id,
                req->rootfs,
                req->command,
                req->soft_limit_bytes >> 20,
                req->hard_limit_bytes >> 20,
                req->nice_value);

        if (write(fd, line, strlen(line)) < 0) {
            perror("write");
            close(fd);
            return 1;
        }

        int stop_sent = 0;

        char buf[4096];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0) {
            fwrite(buf, 1, (size_t)n, stdout);

            if (g_run_interrupt && !stop_sent) {
                stop_sent = 1;
                (void)send_stop_request_simple(req->container_id);
                // keep waiting for final status: do NOT close fd
            }
        }

        close(fd);
        return 0;
    }

    static int cmd_run(int argc, char *argv[])
    {
        control_request_t req;

        if (argc < 5) {
            fprintf(stderr,
                    "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                    argv[0]);
            return 1;
        }

        memset(&req, 0, sizeof(req));
        req.kind = CMD_START; /* run becomes start+wait */
        strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
        strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
        strncpy(req.command, argv[4], sizeof(req.command) - 1);
        req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
        req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
        req.nice_value = 0;

        if (parse_optional_flags(&req, argc, argv, 5) != 0)
            return 1;

        /* Install signal handler: Ctrl+C triggers stop forwarding but we keep waiting */
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = handle_run_client_sig;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);

        g_run_interrupt = 0;

        /* 1) Start container */
        if (send_control_request(&req) != 0) {
            return 1;
        }

        /* 2) Poll ps until terminal state */
        int stop_sent = 0;
        while (1) {
            if (g_run_interrupt && !stop_sent) {
                stop_sent = 1;
                (void)send_stop_request_simple(req.container_id);
            }

            char psbuf[8192];
            if (request_ps_and_capture(psbuf, sizeof(psbuf)) != 0) {
                /* If supervisor is temporarily unavailable, wait and retry */
                usleep(200 * 1000);
                continue;
            }

            char state[64];
            if (ps_find_state(psbuf, req.container_id, state, sizeof(state)) == 0) {
                if (is_terminal_state(state)) {
                    /* Print final state line for the screenshot */
                    printf("OK final state %s\n", state);
                    return 0;
                }
            }

            usleep(200 * 1000); /* 200ms polling interval */
        }
    }

    static int cmd_ps(void)
    {
        control_request_t req;

        memset(&req, 0, sizeof(req));
        req.kind = CMD_PS;

        /*
        * TODO:
        * The supervisor should respond with container metadata.
        * Keep the rendering format simple enough for demos and debugging.
        */
        printf("Expected states include: %s, %s, %s, %s, %s\n",
            state_to_string(CONTAINER_STARTING),
            state_to_string(CONTAINER_RUNNING),
            state_to_string(CONTAINER_STOPPED),
            state_to_string(CONTAINER_KILLED),
            state_to_string(CONTAINER_EXITED));
        return send_control_request(&req);
    }

    static int cmd_logs(int argc, char *argv[])
    {
        control_request_t req;

        if (argc < 3) {
            fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
            return 1;
        }

        memset(&req, 0, sizeof(req));
        req.kind = CMD_LOGS;
        strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

        return send_control_request(&req);
    }

    static int cmd_stop(int argc, char *argv[])
    {
        control_request_t req;

        if (argc < 3) {
            fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
            return 1;
        }

        memset(&req, 0, sizeof(req));
        req.kind = CMD_STOP;
        strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

        return send_control_request(&req);
    }

    int main(int argc, char *argv[])
    {
        if (argc < 2) {
            usage(argv[0]);
            return 1;
        }

        if (strcmp(argv[1], "supervisor") == 0) {
            if (argc < 3) {
                fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
                return 1;
            }
            return run_supervisor(argv[2]);
        }

        if (strcmp(argv[1], "start") == 0)
            return cmd_start(argc, argv);

        if (strcmp(argv[1], "run") == 0)
            return cmd_run(argc, argv);

        if (strcmp(argv[1], "ps") == 0)
            return cmd_ps();

        if (strcmp(argv[1], "logs") == 0)
            return cmd_logs(argc, argv);

        if (strcmp(argv[1], "stop") == 0)
            return cmd_stop(argc, argv);

        usage(argv[0]);
        return 1;
    }

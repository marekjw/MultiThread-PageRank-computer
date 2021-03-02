#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <iostream>

class Sha256IdGenerator : public IdGenerator {
private:
    static void error_exit(std::string reason)
    {
        std::cerr << reason << " " << errno << ": " << std::strerror(errno)
                  << std::endl;
        exit(1);
    }

public:
    virtual PageId generateId(std::string const& content) const
    {

        const int writing_end = 1, reading_end = 0;

        int parent_to_child[2], child_to_parent[2];

        if (pipe2(parent_to_child, O_CLOEXEC) == -1)
            error_exit("Error: pipe parent_to_child creation");

        if (pipe2(child_to_parent, O_CLOEXEC) == -1)
            error_exit("Error: pipe chil_to_parent creation");

        char sha256sum[] = "sha256sum";
        char* args[] = { sha256sum, NULL };

        switch (fork()) {
        case -1:
            error_exit("Error: fork()");
            break;

        case 0:
            if (close(fileno(stdin)) != 0)
                error_exit("Error: closing stdin");
            if (dup(parent_to_child[reading_end]) == -1)
                error_exit("Error: dup parent_to_child");
            if (close(parent_to_child[reading_end]) != 0)
                error_exit("Error: child -> close parent_to_child");

            if (close(fileno(stdout)) != 0)
                error_exit("Error: closing stdout");
            if (dup(child_to_parent[writing_end]) == -1)
                error_exit("Error: dup child_to_parent");
            if (close(child_to_parent[writing_end]) != 0)
                error_exit("Error: child->close child_to_parent");

            if (close(parent_to_child[writing_end]) != 0 || close(child_to_parent[reading_end]) != 0)
                error_exit("Error: child -> close unused pipes");

            execvp(sha256sum, args);

            error_exit("Error in exec");
            break;

        default:

            if (close(parent_to_child[reading_end]) != 0 || close(child_to_parent[writing_end]) != 0)
                error_exit("Error: parent -> close unused pipes");

            if (write(parent_to_child[writing_end], content.c_str(),
                    content.size())
                == -1)
                error_exit("Error: write( )");

            if (close(parent_to_child[writing_end]) != 0)
                error_exit("Error: parent-> close parent_to_child[1]");

            char res[64];

            if (wait(0) == -1)
                error_exit("Error: wait(0)");

            if (read(child_to_parent[reading_end], res, 64) == -1)
                error_exit("Error: parent -> read");

            if (close(child_to_parent[reading_end]) != 0)
                error_exit("Error: parent -> close pipes after reading");

            return PageId(std::string(res, 64));
        }
        return PageId("");
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */

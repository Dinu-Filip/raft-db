all:
	cmake --preset debug
	cmake --build --preset debug

release:
	cmake --preset release
	cmake --build --preset release

clean:
	rm -rf build

format:
	find . -name "*.[ch]" | xargs clang-format -i

GREEN := \033[0;32m
RED := \033[0;31m
RESET:= \033[0m
ESCAPE := \r\033[K

format-newline:
	@EXIT=0; \
	for file in $$(find src -type f); do \
        if tail -c 1 $$file | grep -q "^$$"; then \
			echo -e "$(GREEN)$$file contains a newline$(RESET)"; \
		else \
			echo -e "$(RED)$$file doesn't contain a newline$(RESET)"; \
			EXIT=1; \
		fi; \
	done; \
	exit $$EXIT;

define VALGRIND_RUNNER
	run_valgrind() { \
		if [ -n "$$(git status --porcelain)" ]; then \
			echo "The following files must be committed before running:"; \
			git status --porcelain; \
			exit 1; \
		fi; \
		OLD_BRANCH=$$(git branch --show-current); \
		echo "Changing branch from $$OLD_BRANCH to armv8_testsuite"; \
		git checkout armv8_testsuite; \
		trap 'echo "\nChanging branch from armv8_testsuite to $$OLD_BRANCH"; git checkout $$OLD_BRANCH; exit' INT; \
		cd testsuite/test; \
		FILE_COUNT=$$(find -name "*.$$1" | wc -l); \
		COUNTER=0; \
		PASSED=0; \
		FAILED=0; \
		for file in $$(find -name "*.$$1"); do \
			printf "Total: $$COUNTER/$$FILE_COUNT, Passed: $$PASSED/$$COUNTER, Failed: $$FAILED/$$COUNTER"; \
			COUNTER=$$((COUNTER + 1)); \
			OUTPUT=$$(valgrind ../../build/debug/src/$$2/$$3 $$file /dev/null 2>&1); \
			DISPLAY_FILE=$${file#$$4}; \
			DISPLAY_FILE=$${DISPLAY_FILE%$$5}; \
			if echo "$$OUTPUT" 2>&1 | grep -q "All heap blocks were freed -- no leaks are possible"; then \
				PASSED=$$((PASSED + 1)); \
				echo -e "$(ESCAPE)$(GREEN)$$DISPLAY_FILE passed valgrind tests for $$2$(RESET)"; \
			else \
				FAILED=$$((FAILED + 1)); \
				echo -e "$(ESCAPE)$(RED)$$DISPLAY_FILE failed valgrind tests for $$2"; \
				echo "Valgrind Output:"; \
				echo "$$OUTPUT"; \
				printf "$(RESET)"; \
			fi; \
		done; \
		echo "Passed: $$PASSED/$$COUNTER, Failed: $$FAILED/$$COUNTER"; \
		echo "Changing branch from armv8_testsuite to $$OLD_BRANCH"; \
		git checkout $$OLD_BRANCH; \
	}
endef

valgrind-emulator: all
	@$(VALGRIND_RUNNER); run_valgrind "bin" "emulator" "emulate" "???????????????????" "????????"

valgrind-assembler: all
	@$(VALGRIND_RUNNER); run_valgrind "s" "assembler" "assemble" "?????????????" "??"

valgrind: valgrind-assembler valgrind-emulator

define IWYU_RUNNER
	run_iwyu() { \
        for file in $$(find $$1 -name "*.[ch]" | grep -v "third-party"); do \
			DISPLAY_FILE=$${file%??}; \
			if [ -e $$DISPLAY_FILE.c ] && [ $${file: -1} == "h" ]; then \
				continue; \
			fi; \
			OUTPUT=$$(include-what-you-use -I lib -I $$1 -w -Xiwyu --mapping_file=../iwyu.imp $$file 2>&1); \
			if echo $$OUTPUT | grep -q "should remove these lines"; then \
				echo -e "$(RED)=== $$DISPLAY_FILE ===$(RESET)"; \
				IWYU_EXIT=1; \
			else \
				echo -e "$(GREEN)=== $$DISPLAY_FILE ===$(RESET)"; \
			fi; \
			echo "$$OUTPUT"; \
        done; \
	}
endef

iwyu:
	@cd src; \
	IWYU_EXIT=0; \
	$(IWYU_RUNNER); \
	run_iwyu lib; \
	run_iwyu assembler; \
	run_iwyu emulator; \
	exit $$IWYU_EXIT

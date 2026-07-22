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
	run_iwyu distributed-database; \
	exit $$IWYU_EXIT

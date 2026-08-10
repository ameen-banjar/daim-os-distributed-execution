CC ?= cc
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
CPPFLAGS ?= -Iinclude -I../vendor/DAIM-OS-v1.0.0
LDLIBS ?= -lpthread

BUILD := build
PIC_BUILD := $(BUILD)/pic
CORE_OBJ := $(BUILD)/daim_core.o
MOCK_OBJ := $(BUILD)/mock_switch_adapter.o
OVS_OBJ := $(BUILD)/ovs_switch_adapter.o
PERSISTENT_OBJ := $(BUILD)/ovs_persistent_adapter.o
LEARNING_OBJ := $(BUILD)/daim_learning_app.o
DIST_STATE_OBJ := $(BUILD)/daim_distributed_state.o
PEER_PROTO_OBJ := $(BUILD)/daim_peer_protocol.o
PEER_TRANSPORT_OBJ := $(BUILD)/daim_peer_transport.o
DIST_LEARNING_OBJ := $(BUILD)/daim_distributed_learning_app.o
PIC_CORE_OBJ := $(PIC_BUILD)/daim_core.o
PIC_OVS_OBJ := $(PIC_BUILD)/ovs_switch_adapter.o
PIC_PERSISTENT_OBJ := $(PIC_BUILD)/ovs_persistent_adapter.o
PIC_LEARNING_OBJ := $(PIC_BUILD)/daim_learning_app.o
PIC_DIST_STATE_OBJ := $(PIC_BUILD)/daim_distributed_state.o
PIC_PEER_PROTO_OBJ := $(PIC_BUILD)/daim_peer_protocol.o
PIC_PEER_TRANSPORT_OBJ := $(PIC_BUILD)/daim_peer_transport.o
PIC_DIST_LEARNING_OBJ := $(PIC_BUILD)/daim_distributed_learning_app.o
TESTS := $(BUILD)/test_core $(BUILD)/test_adapters $(BUILD)/test_concurrency $(BUILD)/test_learning_app $(BUILD)/test_persistent_adapter $(BUILD)/test_distributed_state $(BUILD)/test_peer_protocol $(BUILD)/test_peer_transport $(BUILD)/test_distributed_learning_app
TOOLS := $(BUILD)/daim_ovs_flow $(BUILD)/test_persistent_live $(BUILD)/daim_persistent_flow
SHARED_LIB := $(BUILD)/libdaim_core.so
SHARED_LIB_DIST := $(BUILD)/libdaim_distributed.so

.PHONY: all check tsan clean

all: $(TESTS) $(TOOLS) $(SHARED_LIB) $(SHARED_LIB_DIST)

check: $(TESTS)
	$(BUILD)/test_core
	$(BUILD)/test_adapters
	$(BUILD)/test_concurrency
	$(BUILD)/test_learning_app
	$(BUILD)/test_persistent_adapter
	$(BUILD)/test_distributed_state
	$(BUILD)/test_peer_protocol
	$(BUILD)/test_peer_transport
	$(BUILD)/test_distributed_learning_app

tsan:
	$(MAKE) BUILD=build-tsan CC=clang \
		CFLAGS="-std=c11 -Wall -Wextra -Wpedantic -Werror -O1 -g -fsanitize=thread" \
		LDLIBS="-lpthread -fsanitize=thread" check

$(BUILD):
	mkdir -p $(BUILD)

$(PIC_BUILD):
	mkdir -p $(PIC_BUILD)

$(CORE_OBJ): src/daim_core.c include/daim_core.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(MOCK_OBJ): src/mock_switch_adapter.c include/daim_switch_adapter.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(OVS_OBJ): src/ovs_switch_adapter.c include/ovs_switch_adapter.h include/daim_switch_adapter.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(PERSISTENT_OBJ): src/ovs_persistent_adapter.c include/ovs_persistent_adapter.h include/daim_switch_adapter.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(LEARNING_OBJ): src/daim_learning_app.c include/daim_learning_app.h include/daim_core.h include/daim_switch_adapter.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(DIST_STATE_OBJ): src/daim_distributed_state.c include/daim_distributed_state.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(PEER_PROTO_OBJ): src/daim_peer_protocol.c include/daim_peer_protocol.h include/daim_distributed_state.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(PEER_TRANSPORT_OBJ): src/daim_peer_transport.c include/daim_peer_transport.h include/daim_peer_protocol.h include/daim_distributed_state.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(DIST_LEARNING_OBJ): src/daim_distributed_learning_app.c include/daim_distributed_learning_app.h include/daim_distributed_state.h include/daim_peer_transport.h include/daim_core.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(PIC_CORE_OBJ): src/daim_core.c include/daim_core.h | $(PIC_BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

$(PIC_OVS_OBJ): src/ovs_switch_adapter.c include/ovs_switch_adapter.h include/daim_switch_adapter.h | $(PIC_BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

$(PIC_PERSISTENT_OBJ): src/ovs_persistent_adapter.c include/ovs_persistent_adapter.h include/daim_switch_adapter.h | $(PIC_BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

$(PIC_LEARNING_OBJ): src/daim_learning_app.c include/daim_learning_app.h include/daim_core.h include/daim_switch_adapter.h | $(PIC_BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

$(PIC_DIST_STATE_OBJ): src/daim_distributed_state.c include/daim_distributed_state.h | $(PIC_BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

$(PIC_PEER_PROTO_OBJ): src/daim_peer_protocol.c include/daim_peer_protocol.h include/daim_distributed_state.h | $(PIC_BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

$(PIC_PEER_TRANSPORT_OBJ): src/daim_peer_transport.c include/daim_peer_transport.h include/daim_peer_protocol.h include/daim_distributed_state.h | $(PIC_BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

$(PIC_DIST_LEARNING_OBJ): src/daim_distributed_learning_app.c include/daim_distributed_learning_app.h include/daim_distributed_state.h include/daim_peer_transport.h include/daim_core.h | $(PIC_BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -fPIC -c $< -o $@

$(BUILD)/test_core: tests/test_core.c $(CORE_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_adapters: tests/test_adapters.c $(MOCK_OBJ) $(OVS_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_concurrency: tests/test_concurrency.c $(CORE_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_learning_app: tests/test_learning_app.c $(LEARNING_OBJ) $(CORE_OBJ) $(MOCK_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_persistent_adapter: tests/test_persistent_adapter.c $(PERSISTENT_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_distributed_state: tests/test_distributed_state.c $(DIST_STATE_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_peer_protocol: tests/test_peer_protocol.c $(PEER_PROTO_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_peer_transport: tests/test_peer_transport.c $(PEER_TRANSPORT_OBJ) $(PEER_PROTO_OBJ) $(DIST_STATE_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_distributed_learning_app: tests/test_distributed_learning_app.c $(DIST_LEARNING_OBJ) $(DIST_STATE_OBJ) $(PEER_TRANSPORT_OBJ) $(PEER_PROTO_OBJ) $(CORE_OBJ) $(MOCK_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/daim_ovs_flow: tools/daim_ovs_flow.c $(OVS_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/test_persistent_live: tools/test_persistent_live.c $(PERSISTENT_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(BUILD)/daim_persistent_flow: tools/daim_persistent_flow.c $(PERSISTENT_OBJ)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(SHARED_LIB): $(PIC_CORE_OBJ) $(PIC_OVS_OBJ) $(PIC_PERSISTENT_OBJ) $(PIC_LEARNING_OBJ)
	$(CC) -shared -fPIC $^ $(LDLIBS) -o $@

$(SHARED_LIB_DIST): $(PIC_CORE_OBJ) $(PIC_OVS_OBJ) $(PIC_PERSISTENT_OBJ) $(PIC_DIST_STATE_OBJ) $(PIC_PEER_PROTO_OBJ) $(PIC_PEER_TRANSPORT_OBJ) $(PIC_DIST_LEARNING_OBJ)
	$(CC) -shared -fPIC $^ $(LDLIBS) -o $@

clean:
	rm -f $(BUILD)/*.o $(PIC_BUILD)/*.o $(TESTS) $(TOOLS) $(SHARED_LIB) $(SHARED_LIB_DIST)

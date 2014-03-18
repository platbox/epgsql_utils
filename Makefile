APPNAME = mc_banking
REBAR ?= $(shell which rebar 2>/dev/null || which ./rebar)
DIALYZER = dialyzer

TARGET_DIR = /opt/platbox/mts_mc_banking
TARGET_USER = banking

.PHONY: all compile deps clean distclean eunit release deploy

all: compile

deps: $(REBAR)
	$(REBAR) get-deps

compile: deps
	$(REBAR) compile

dc:
	$(REBAR) compile skip_deps=true

start:
	erl -pa apps/*/ebin deps/*/ebin -config dev -s banking -s reloader

eunit: dc
	$(REBAR) eunit skip_deps=true

clean: $(REBAR)
	$(REBAR) clean

distclean: clean
	$(REBAR) delete-deps
	rm -rfv plts

datamodel: escriptize
	$(REBAR) escriptize

release: compile
	mkdir -p rel_build
	cd rel && rm -rf $(APPNAME) && ../$(REBAR) generate && rsync -ra $(APPNAME) ../rel_build

deploy: release
	mkdir -p $(TARGET_DIR)
	rsync -ra rel_build/$(APPNAME)/* $(TARGET_DIR)/. && chown -R $(TARGET_USER):$(TARGET_USER) $(TARGET_DIR)

## dialyzer

~/.dialyzer_plt:
	dialyzer --build_plt --output_plt ~/.dialyzer_plt --apps `ls /usr/lib/erlang/lib/ -1 | awk -F "-" '{print $$1}' | sed '/erl_interface/d' | sed '/jinterface/d'`; true

plts/otp.plt: ~/.dialyzer_plt
	mkdir -p plts && cp ~/.dialyzer_plt plts/otp.plt

plts/deps.plt: plts/otp.plt
	rm -rf `find deps -name ".eunit"`
	$(DIALYZER) --add_to_plt --plt plts/otp.plt --output_plt plts/deps.plt -r deps; true

dialyzer: compile plts/deps.plt
	rm -rf `find apps -name ".eunit"`
	$(DIALYZER) --plt plts/deps.plt -n --no_native -r apps; true

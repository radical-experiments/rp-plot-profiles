#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp
import radical.utils as ru

RESOURCES = {'local' : {
                 'resource' : 'local.localhost',
                 'project'  : None,
                 'queue'    : None,
                 'schema'   : None,
                 },
             
             'test' : {
                 'resource' : 'home.test',
                 'project'  : None,
                 'queue'    : None,
                 'schema'   : 'ssh',
                 },
             
             'archer' : {
                 'resource' : 'epsrc.archer',
                 'project'  : 'e290',
                 'queue'    : 'short',
                 'schema'   : None,
                 },
             
             'supermuc' : {
                 'resource' : 'lrz.supermuc',
                 'project'  : 'e290',
                 'queue'    : 'short',
                 'schema'   : None,
                 },
             
             'stampede' : {
                 'resource' : 'xsede.stampede',
                 'project'  : 'TG-MCB090174' ,
                 'queue'    : 'normal',
                 'schema'   : None,
                 },
             
             'gordon' : {
                 'resource' : 'xsede.gordon',
                 'project'  : None,
                 'queue'    : 'debug',
                 'schema'   : None,
                 },
             
             'blacklight' : {
                 'resource' : 'xsede.blacklight',
                 'project'  : None,
                 'queue'    : 'debug',
                 'schema'   : 'gsissh',
                 },
             
             'trestles' : {
                 'resource' : 'xsede.trestles',
                 'project'  : 'TG-MCB090174' ,
                 'queue'    : 'shared',
                 'schema'   : None,
                 },

             'india' : {
                 'resource' : 'futuregrid.india',
                 'project'  : None,
                 'queue'    : None,
                 'schema'   : None,
                 },

             'hopper' : {
                 'resource' : 'nersc.hopper',
                 'project'  : None,
                 'queue'    : 'debug',
                 'schema'   : 'ssh',
                 }
             }

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return


    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:
        print "[Callback]: %s" % state


    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        sys.exit(2)


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print "[Callback]: wait_queue_size: %s." % wait_queue_size


#------------------------------------------------------------------------------
#
def run_experiment(n_cores, n_units, resources, runtime, cu_load, agent_config, 
        scheduleri, queue=None):

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    sid     = session.uid
    print "session id: %s" % sid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        pmgr = rp.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        pilots = list()

        for resource in resources:

            if not queue: 
                queue = RESOURCES[resource]['queue']

            pdesc = rp.ComputePilotDescription()
            pdesc.resource      = RESOURCES[resource]['resource']
            pdesc.cores         = n_cores
            pdesc.project       = RESOURCES[resource]['project']
            pdesc.queue         = queue
            pdesc.runtime       = runtime
            pdesc.cleanup       = False
            pdesc.access_schema = RESOURCES[resource]['schema']
            pdesc._config       = agent_config

            pilot = pmgr.submit_pilots(pdesc)

            input_sd_pilot = {
                    'source': 'file:///etc/passwd',
                    'target': 'staging:///f1',
                    'action': rp.TRANSFER
                    }
          # pilot.stage_in (input_sd_pilot)

            pilots.append (pilot)


        umgr = rp.UnitManager(session=session, scheduler=scheduler)
        umgr.register_callback(unit_state_cb,      rp.UNIT_STATE)
        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)
        umgr.add_pilots(pilots)

        input_sd_umgr   = {'source':'/etc/group',    'target': 'f2',                'action': rp.TRANSFER}
        input_sd_agent  = {'source':'staging:///f1', 'target': 'f1',                'action': rp.COPY}
        output_sd_agent = {'source':'f1',            'target': 'staging:///f1.bak', 'action': rp.COPY}
        output_sd_umgr  = {'source':'f2',            'target': 'f2.bak',            'action': rp.TRANSFER}

        cuds = list()
        for unit_count in range(0, n_units):
            cud = rp.ComputeUnitDescription()
            cud.executable     = cu_load['executable']
            cud.arguments      = cu_load['arguments']
            cud.cores          = cu_load['cores']
            cud.input_staging  = cu_load.get('inputs')
            cud.output_staging = cu_load.get('outputs')
            cuds.append(cud)

        units = umgr.submit_units(cuds)

        umgr.wait_units()

        for cu in units:
            print "* Task %s state %s, exit code: %s, started: %s, finished: %s" \
                % (cu.uid, cu.state, cu.exit_code, cu.start_time, cu.stop_time)

      # os.system ("radicalpilot-stats -m stat,plot -s %s > %s.stat" % (session.uid, session_name))


    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        session.close (cleanup=False) # keep the DB entries...


    return sid

#-------------------------------------------------------------------------------


if __name__ == "__main__":

    import optparse
    parser = optparse.OptionParser (add_help_option=False)
    
    parser.add_option('-c', '--cores',     dest='cores')
    parser.add_option('-u', '--units',     dest='units')
    parser.add_option('-t', '--time',      dest='runtime')
    parser.add_option('-l', '--load',      dest='load')
    parser.add_option('-a', '--agent',     dest='agent')
    parser.add_option('-r', '--resources', dest='resources')
    parser.add_option('-s', '--scheduler', dest='scheduler')
    parser.add_option('-q', '--queue',     dest='queue')
    
    options, args = parser.parse_args ()
    
    n_cores   = options.cores
    n_units   = options.units
    resources = options.resources
    runtime   = options.runtime
    load      = options.load
    agent     = options.agent
    scheduler = options.scheduler
    queue     = options.queue

    
    if   scheduler == 'direct'     : scheduler = rp.SCHED_DIRECT
    elif scheduler == 'backfilling': scheduler = rp.SCHED_BACKFILLING
    elif scheduler == 'round_robin': scheduler = rp.SCHED_ROUND_ROBIN
    else                           : scheduler = rp.SCHED_ROUND_ROBIN
    
    if not n_cores  : raise ValueError ("need number of cores")
    if not n_units  : raise ValueError ("need number of units")
    if not runtime  : raise ValueError ("need pilot runtime")
    if not resources: raise ValueError ("need target resource")
    if not load     : raise ValueError ("need load config")
    if not agent    : raise ValueError ("need agent config")
    
    resources = resources.split(',')

    for resource in resources:
        if not resource in RESOURCES:
            raise ValueError ("unknown resource %s" % resource)
    
    cu_load      = ru.read_json (load)
    agent_config = ru.read_json (agent)

    n_cores = int(n_cores)
    n_units = int(n_units)
    runtime = int(runtime)

    sid = run_experiment (n_cores, n_units, resources, runtime, cu_load,
            agent_config, scheduler, queue)

    print "session id: %s" % sid


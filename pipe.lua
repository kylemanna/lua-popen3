#!/usr/bin/env lua
--
-- Name: Lua 5.2 + popen3() implementation
-- Author: Kyle Manna <kyle [at] kylemanna.com>
-- License: MIT License <http://opensource.org/licenses/MIT>
-- Copyright (c) 2013 Kyle Manna
--
-- Description:
-- Open pipes for stdin, stdout, and stderr to a forked process
-- to allow for IPC with the process.  When the process terminates
-- return the status code.
--
-- Includes a pipe_multi() wrapper that is simple, straight to the point.
--
 
--
-- Simple popen3() implementation
--
function popen3(path, ...)
	local r1, w1 = posix.pipe()
	local r2, w2 = posix.pipe()
	local r3, w3 = posix.pipe()

	assert((w1 ~= nil or r2 ~= nil or r3 ~= nil), "pipe() failed")

	local pid, err = posix.fork()
	assert(pid ~= nil, "fork() failed")
	if pid == 0 then
		posix.close(w1)
		posix.close(r2)
		posix.close(r3)

		posix.dup2(r1, posix.fileno(io.stdin))
		posix.dup2(w2, posix.fileno(io.stdout))
		posix.dup2(w3, posix.fileno(io.stderr))

		local ret, err = posix.execp(path, ...)
		assert(ret ~= nil, "execp() failed")

		posix._exit(1)
		return
	end

	posix.close(r1)
	posix.close(w2)
	posix.close(w3)

	return pid, w1, r2, r3
end

--
-- Pipe an array of input into the same cmd + optional arguments (each
-- cmd is forked) and wait for completion and then return status code,
-- stdout and stderr from cmd.
--
-- Note: This functions expects @input to be an array starting at index
-- 1 such that #input operator works.
--
function pipe_multi(input, max_procs, cmd, ...)

	local stdout_fd = {}
	local stderr_fd = {}
	local stdout = {}
	local stderr = {}
	local status = {}
	local lookup_pid2idx = {} -- translate pid to array index
	local lookup_fd2table = {} -- translate pid to array index
	local idx = 0
	local inflight = 0
	local remaining = #input
	local proc = {}
	local bufsize = 8192 -- (Never exceed 64k or we'll hit OS limits on pipes)
	local wait = {}

	-- Loop until we have a status for each input ie #status == #input
	while remaining > 0 do
		--print ("pipe: remaining = "..remaining)

		-- Launch each child process and pipe stdin to it
		while inflight < max_procs and idx < #input do
			idx = idx + 1
			local lpid, lstdin_fd, lstdout_fd, lstderr_fd = popen3(cmd, ...)
			proc[idx] = {}
			proc[idx]['pid'] = lpid
			proc[idx]['stdin_fd'] = lstdin_fd
			proc[idx]['stdout_fd'] = lstdout_fd
			proc[idx]['stderr_fd'] = lstderr_fd
			proc[idx]['stdin'] = input[idx]
			proc[idx]['stdin_pos'] = 1
			proc[idx]['stdout'] = {}
			proc[idx]['stderr'] = {}

			assert(lpid ~= nil, "filter() unable to popen3()")

			--print("pipe: spawned pid = "..lpid.." with cmd = "..cmd.." for input idx = "..idx)

			lookup_fd2table[proc[idx]['stdin_fd']] = proc[idx]
			lookup_fd2table[proc[idx]['stdout_fd']] = proc[idx]
			lookup_fd2table[proc[idx]['stderr_fd']] = proc[idx]

			lookup_pid2idx[lpid] = idx
			--lookup_idx2pid[idx] = lpid
			inflight = inflight + 1
		end

		-- Write to popen3's stdin, important to close it as some (most?) proccess
		-- block until the stdin pipe is closed.  Also make sure stdout and stderr
		-- don't backup or the child will be blocked by this process and could
		-- ultimately lead to deadlock if this process is stuck in wait()
		--
		-- If a child has completed (and the wait table has elements), don't loop
		-- so that the wait code can clean-up.
		--
		while not next(wait) do

			local fds = {}
			for _,p in pairs(proc) do
				if p['stdin_fd'] ~= nil then fds[p['stdin_fd']] = { events = {OUT=true} } end
				if p['stdout_fd'] ~= nil then fds[p['stdout_fd']] = { events = {IN=true} } end
				if p['stderr_fd'] ~= nil then fds[p['stderr_fd']] = { events = {IN=true} } end
			end

			if next(fds) then
				local fds_ready = posix.poll(fds, -1)
				if fds_ready == 0 then break end
			else
				break
			end


			for fd in pairs(fds) do

				-- Read stdout or stderr from child
				if fds[fd].revents.IN then

					local mytable = lookup_fd2table[fd]
					table.insert(mytable['stdout'], posix.read(fd, bufsize))

				-- Write stdin to child
				elseif fds[fd].revents.OUT then

					local mytable = lookup_fd2table[fd]
					local pos = mytable['stdin_pos']
					local substr = mytable['stdin']:sub(pos, pos + bufsize - 1)
					local nbytes, err = posix.write(fd, substr)
					pos = pos + nbytes
					--print('pipe: trying to write '..#substr..' bytes to child['..fd..'], actually wrote '..nbytes..', pos =  '..pos)
					mytable['stdin_pos'] = pos

					-- When max position is exceeded, close the fd
					if mytable['stdin_pos'] > #mytable['stdin'] then
						--print('>> Closing stdin for pid = '..mytable['pid']..' fd = '..fd)
						posix.close(fd)
						mytable['stdin_fd'] = nil
					end

				-- When the child exits, it will send the HUP signal, close fds
				-- and mark as closed to prevent further poll()ing on it (NVAL)
				elseif fds[fd].revents.HUP then

					--print('Closing fd = '..fd)

					posix.close(fd)
					local mytable = lookup_fd2table[fd]
					if     mytable['stdout_fd'] == fd then mytable['stdout_fd'] = nil
					elseif mytable['stderr_fd'] == fd then mytable['stderr_fd'] = nil end

					-- Once chiled has exited, the parent can't clean it up
					if mytable['stdout_fd'] == nil and mytable['stderr_fd'] == nil then
						wait[mytable['pid']] = mytable['pid']
					end

				-- Something unexpected happend
				else

					-- If we make it here, spend some time and figure out why
					for k,v in  pairs(fds[fd].revents) do
						assert(v == false, 'Unhandled case, revent = '..k)
					end

				end
			end

		end

		for k, pid in pairs(wait) do
			--print('About to wait, pid = '..pid)
			-- Only want to wait when no other IO is pending or a deadlock could occur
			-- where this process waits for a child to terminate, but the child won't
			-- terminate until its pipes are empty or closed.
			local wait_pid, wait_cause, wait_status = posix.wait(pid)
			assert(wait_pid ~= nil)
			local wait_idx = lookup_pid2idx[wait_pid]

			--print ("pipe: pid = "..wait_pid.." completed for input idx = "..wait_idx)
			local mytable = proc[lookup_pid2idx[wait_pid]]

			-- Make sure that poll correctly handled all the file handles
			assert(mytable['stdin_fd'] == nil)
			assert(mytable['stdout_fd'] == nil)
			assert(mytable['stderr_fd'] == nil)

			-- Setup the outgoing tables
			status[wait_idx] = wait_status
			stdout[wait_idx] = table.concat(mytable['stdout'])
			stderr[wait_idx] = table.concat(mytable['stderr'])

			inflight = inflight - 1
			remaining = remaining - 1
		end
		wait = {}

	end

	-- Checks for correctness
	assert(#input == idx, "#input = "..#input.." idx = "..idx)
	assert(#input == #status, "#input = "..#input.." #status = "..#status)
	assert(inflight == 0, "inflight = "..inflight)
	assert(remaining == 0, "remaining = "..remaining)

	return status, stdout, stderr
end

--
-- Special case of pipe_multi() where we don't use tables for input
-- or output since we have only one process to run.
--
function pipe_single(input, cmd, ...)
	local status, stdout, stderr = pipe_multi({input}, 1, cmd, ...)
	return status[1], stdout[1], stderr[1]
end

# 20261012-2024-pty-flow-control Terminal flow control

- **status**: pending
- **priority**: P2
- **owner**: (unassigned)
- **createdAt**: 2026-10-12 20:24

## Description

Terminal input and output have no flow control. Input beyond the agent's 64-chunk queue ends
the terminal with `input_overflow`; output beyond the host's 256 KiB unread buffer, or beyond
the browser bridge's send buffer, ends it with `slow_consumer`. A large paste, or a large
output burst read over a link slower than the agent's, therefore closes the terminal.

Acceptance criteria: a writer can tell when input is accepted or must wait, and a slow reader
slows the terminal's output instead of ending it. Design to be proposed (for example a
credit window per terminal).

## ActiveForm

Adding terminal flow control

## Dependencies

- **blocked by**: (none)
- **blocks**: (none)

## Notes

Found by the review recorded in `20261012-2019-remote-ops-review-fixes`.

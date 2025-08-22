#ifndef WORKFLOW_H
#define WORKFLOW_H

enum class Op : uint8_t
{
  Accept,
  Recv,
  Send,
  Kick,
  Process,
  Respond
};

#endif // WORKFLOW_H
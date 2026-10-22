import { Fragment, useMemo } from "react"
import { ChevronRightIcon, EyeIcon, UsersIcon } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { ContextBadge } from "@/features/events/context-badge"
import type { ConsumerGroup, ConsumerGroupMember, ConsumerGroupPartition } from "@/lib/api"
import { cn } from "@/lib/utils"

import { useConsumerGroups } from "./use-kafka-queries"

/**
 * Member colors. Deliberately different from the context colors (sky/amber/emerald/violet),
 * which already mean "topic" on this page.
 */
const MEMBER_COLORS = [
  "bg-rose-500",
  "bg-teal-500",
  "bg-fuchsia-500",
  "bg-lime-500",
  "bg-orange-500",
  "bg-cyan-500",
  "bg-indigo-500",
  "bg-pink-500",
]

const memberColor = (index: number) => MEMBER_COLORS[index % MEMBER_COLORS.length]
const tpKey = (topic: string, partition: number) => `${topic}#${partition}`

export function ConsumerGroupsSection() {
  const { data: groups, error, isPending, isFetching } = useConsumerGroups()

  return (
    <Card className="min-w-0 gap-0 py-0">
      <CardHeader className="gap-1 border-b py-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <CardTitle>Consumer groups</CardTitle>
          <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
            <span
              className={cn(
                "size-1.5 rounded-full bg-emerald-500 transition-opacity",
                isFetching ? "opacity-100" : "opacity-40",
              )}
              aria-hidden
            />
            refresh 2 s
          </span>
        </div>
        <CardDescription>
          Each service reads with its own group, so every group gets every event. Inside a group the partitions are
          split among the members: one partition has exactly one owner, so adding replicas only helps up to the
          number of partitions (3 here). <strong className="font-medium text-foreground">Lag</strong> = end offset −
          committed offset: records written but not yet processed by that group.
        </CardDescription>
      </CardHeader>

      <CardContent className="flex flex-col gap-4 py-4">
        {isPending ? (
          <p className="py-8 text-center text-sm text-muted-foreground">Reading consumer groups…</p>
        ) : !groups ? (
          <p className="py-8 text-center text-sm text-destructive">
            Could not load consumer groups: {error?.message ?? "unknown error"}
          </p>
        ) : groups.length === 0 ? (
          <p className="py-8 text-center text-sm text-muted-foreground">No consumer groups on the broker yet.</p>
        ) : (
          <>
            {error && (
              <p className="text-xs text-destructive">Refresh failed ({error.message}); showing the last result.</p>
            )}
            {groups.filter((g) => !isLeftover(g)).map((group) => (
              <GroupCard key={group.groupId} group={group} />
            ))}
            <LeftoverNote groups={groups.filter(isLeftover)} />
          </>
        )}
      </CardContent>
    </Card>
  )
}

/** No members and nothing committed, e.g. a finished kafka-console-consumer: nothing to show. */
const isLeftover = (g: ConsumerGroup) => g.members.length === 0 && g.partitions.length === 0

function LeftoverNote({ groups }: { groups: ConsumerGroup[] }) {
  if (groups.length === 0) return null
  return (
    <p className="text-xs text-muted-foreground">
      Hidden (no members, no committed offsets):{" "}
      <span className="font-mono">{groups.map((g) => g.groupId).join(", ")}</span>
    </p>
  )
}

function GroupCard({ group }: { group: ConsumerGroup }) {
  // Which member owns which partition, by member index (for the colored owner dots).
  const owners = useMemo(() => {
    const map = new Map<string, number>()
    group.members.forEach((m, i) => {
      for (const a of m.assignments) map.set(tpKey(a.topic, a.partition), i)
    })
    return map
  }, [group.members])

  const maxLag = Math.max(1, ...group.partitions.map((p) => p.lag ?? 0))

  const table = <PartitionTable group={group} owners={owners} maxLag={maxLag} />

  return (
    <section
      aria-label={`Consumer group ${group.groupId}`}
      className={cn("min-w-0 rounded-xl border", group.isObserver ? "border-dashed bg-muted/10" : "bg-muted/20")}
    >
      <header className="flex flex-wrap items-center gap-2 border-b px-3 py-2">
        <span className="font-mono text-sm font-semibold">{group.groupId}</span>
        <StateBadge state={group.state} />
        {group.isObserver && (
          <Badge variant="outline" className="gap-1 text-muted-foreground">
            <EyeIcon aria-hidden />
            observer · never commits
          </Badge>
        )}
        <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
          <UsersIcon className="size-3.5" aria-hidden />
          {group.members.length} {group.members.length === 1 ? "member" : "members"}
        </span>
        <LagBadge group={group} />
      </header>

      <div className="grid min-w-0 gap-3 p-3 lg:grid-cols-[minmax(0,20rem)_minmax(0,1fr)]">
        <MemberList group={group} />
        {group.isObserver ? (
          <div className="flex min-w-0 flex-col gap-2">
            <p className="text-xs text-muted-foreground">
              Playground.Api reads every topic with auto-commit off and never commits, so this group has no committed
              offsets and no lag: each start replays the topics from the beginning. Its members still split the
              partitions like any other group.
            </p>
            <details className="group/details min-w-0">
              <summary className="flex cursor-pointer list-none items-center gap-1 text-xs font-medium select-none [&::-webkit-details-marker]:hidden">
                <ChevronRightIcon className="size-3.5 transition-transform group-open/details:rotate-90" />
                Show {group.partitions.length} partitions
              </summary>
              <div className="mt-2">{table}</div>
            </details>
          </div>
        ) : (
          table
        )}
      </div>
    </section>
  )
}

function StateBadge({ state }: { state: string }) {
  const rebalancing = state === "PreparingRebalance" || state === "CompletingRebalance"
  const style =
    state === "Stable"
      ? "border-emerald-500/30 bg-emerald-500/15 text-emerald-700 dark:text-emerald-300"
      : rebalancing
        ? "border-amber-500/40 bg-amber-500/15 text-amber-700 motion-safe:animate-pulse dark:text-amber-300"
        : state === "Empty"
          ? "border-border bg-muted text-muted-foreground"
          : "border-destructive/40 bg-destructive/15 text-destructive"
  const label = rebalancing ? `Rebalancing (${state.replace("Rebalance", "")})` : state
  const hint =
    state === "Stable"
      ? "Every member has its partitions and is consuming."
      : rebalancing
        ? "A member joined or left: consumption pauses while partitions are reassigned."
        : state === "Empty"
          ? "No members right now; committed offsets are kept, so the group resumes where it stopped."
          : "The group is gone or its state is unknown."
  return (
    <Badge variant="outline" className={style} title={hint}>
      {label}
    </Badge>
  )
}

function LagBadge({ group }: { group: ConsumerGroup }) {
  if (group.isObserver)
    return (
      <Badge variant="outline" className="ml-auto text-muted-foreground" title="No commits, so no lag">
        lag n/a
      </Badge>
    )
  const lag = group.totalLag
  return (
    <Badge
      variant="outline"
      className={cn(
        "ml-auto tabular-nums",
        lag === 0
          ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
          : "border-amber-500/40 bg-amber-500/15 text-amber-700 dark:text-amber-300",
      )}
      title="Sum of end offset − committed offset over all partitions"
    >
      total lag {lag}
    </Badge>
  )
}

function MemberList({ group }: { group: ConsumerGroup }) {
  if (group.members.length === 0)
    return (
      <p className="rounded-lg border border-dashed p-3 text-xs text-muted-foreground">
        No members: nobody in this group is running. Its committed offsets stay on the broker, so lag grows until a
        member comes back and resumes from them.
      </p>
    )
  return (
    <ul className="flex min-w-0 flex-col gap-2" aria-label="Members">
      {group.members.map((member, i) => (
        <MemberItem key={member.memberId} member={member} index={i} />
      ))}
    </ul>
  )
}

function MemberItem({ member, index }: { member: ConsumerGroupMember; index: number }) {
  const byTopic = useMemo(() => {
    const map = new Map<string, number[]>()
    for (const a of member.assignments) map.set(a.topic, [...(map.get(a.topic) ?? []), a.partition])
    return [...map.entries()]
  }, [member.assignments])

  return (
    <li className="min-w-0 rounded-lg border bg-card p-2">
      <div className="flex min-w-0 items-center gap-2">
        <span
          className={cn(
            "inline-flex size-5 shrink-0 items-center justify-center rounded text-[11px] font-semibold text-white",
            memberColor(index),
          )}
          aria-hidden
        >
          {index + 1}
        </span>
        <span className="truncate text-sm font-medium" title={`client.id = ${member.clientId}`}>
          {member.clientId}
        </span>
        <span className="truncate font-mono text-xs text-muted-foreground" title="Host the member connects from">
          @{member.host}
        </span>
      </div>
      <div className="mt-0.5 truncate font-mono text-[11px] text-muted-foreground" title={member.memberId}>
        {member.memberId}
      </div>
      <div className="mt-1.5 flex flex-wrap gap-1.5">
        {byTopic.length === 0 ? (
          <span className="text-xs text-muted-foreground italic">
            no partitions assigned (more members than partitions, or a rebalance in progress)
          </span>
        ) : (
          byTopic.map(([topic, partitions]) => (
            <span key={topic} className="inline-flex items-center gap-1">
              <ContextBadge topic={topic} className="h-4 px-1.5 text-[10px]" />
              <span className="font-mono text-xs">{partitions.map((p) => `p${p}`).join(" ")}</span>
            </span>
          ))
        )}
      </div>
    </li>
  )
}

function PartitionTable({
  group,
  owners,
  maxLag,
}: {
  group: ConsumerGroup
  owners: Map<string, number>
  maxLag: number
}) {
  const byTopic = useMemo(() => {
    const map = new Map<string, ConsumerGroupPartition[]>()
    for (const p of group.partitions) map.set(p.topic, [...(map.get(p.topic) ?? []), p])
    return [...map.entries()]
  }, [group.partitions])

  if (group.partitions.length === 0)
    return <p className="text-xs text-muted-foreground">No committed offsets and no assignments.</p>

  return (
    <div className="min-w-0 overflow-x-auto rounded-lg border bg-card">
      <table className="w-full min-w-[26rem] text-xs">
        <thead className="bg-muted/50 text-left text-muted-foreground">
          <tr>
            <th scope="col" className="px-2 py-1.5 font-medium">Partition</th>
            <th scope="col" className="px-2 py-1.5 font-medium">Owner</th>
            <th scope="col" className="px-2 py-1.5 text-right font-medium" title="Next offset the group will read">
              Committed
            </th>
            <th scope="col" className="px-2 py-1.5 text-right font-medium" title="Offset the next record will get">
              End
            </th>
            <th scope="col" className="w-[35%] px-2 py-1.5 font-medium">Lag</th>
          </tr>
        </thead>
        <tbody>
          {byTopic.map(([topic, partitions]) => (
            <Fragment key={topic}>
              <tr className="border-t bg-muted/20">
                <th scope="rowgroup" colSpan={5} className="px-2 py-1 text-left font-normal">
                  <span className="inline-flex items-center gap-2">
                    <ContextBadge topic={topic} className="h-4 px-1.5 text-[10px]" />
                    <span className="font-mono text-muted-foreground">{topic}</span>
                  </span>
                </th>
              </tr>
              {partitions.map((p) => (
                <PartitionRow
                  key={p.partition}
                  row={p}
                  owner={owners.get(tpKey(p.topic, p.partition))}
                  maxLag={maxLag}
                  observer={group.isObserver}
                />
              ))}
            </Fragment>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function PartitionRow({
  row,
  owner,
  maxLag,
  observer,
}: {
  row: ConsumerGroupPartition
  owner: number | undefined
  maxLag: number
  observer: boolean
}) {
  return (
    <tr className="border-t font-mono tabular-nums">
      <td className="px-2 py-1.5">p{row.partition}</td>
      <td className="px-2 py-1.5">
        {owner === undefined ? (
          <span className="font-sans text-muted-foreground" title="Not assigned to any member right now">
            —
          </span>
        ) : (
          <span className="inline-flex items-center gap-1">
            <span className={cn("size-2.5 rounded-sm", memberColor(owner))} aria-hidden />
            <span className="font-sans">#{owner + 1}</span>
          </span>
        )}
      </td>
      <td className="px-2 py-1.5 text-right">{row.committedOffset ?? <span className="text-muted-foreground">—</span>}</td>
      <td className="px-2 py-1.5 text-right">{row.endOffset}</td>
      <td className="px-2 py-1.5">
        {row.lag === null ? (
          <span className="font-sans text-muted-foreground">
            {observer ? "n/a" : row.endOffset === 0 ? "no records yet" : "no commit yet"}
          </span>
        ) : (
          <span className="flex items-center gap-2">
            <span className={cn("w-6 text-right", row.lag > 0 && "font-semibold text-amber-700 dark:text-amber-300")}>
              {row.lag}
            </span>
            {row.lag === 0 ? (
              <span className="font-sans text-emerald-700 dark:text-emerald-400">caught up</span>
            ) : (
              <span className="h-1.5 flex-1 overflow-hidden rounded-full bg-muted" aria-hidden>
                <span
                  className="block h-full rounded-full bg-amber-500 transition-[width] duration-500"
                  style={{ width: `${Math.max(6, (row.lag / maxLag) * 100)}%` }}
                />
              </span>
            )}
          </span>
        )}
      </td>
    </tr>
  )
}

'use client';

import { usePrograms } from '@/hooks/use-programs';
import { ProgramCard } from './program-card';
import { ProgramListSkeleton } from './program-list-skeleton';
import type { TimeRange } from '@/types';

interface ProgramListProps {
  timeRange?: TimeRange;
}

export function ProgramList({ timeRange = '24h' }: ProgramListProps) {
  const { data: programs, isLoading, error } = usePrograms(timeRange);

  if (isLoading) {
    return <ProgramListSkeleton />;
  }

  if (error) {
    return (
      <div className="text-center py-12">
        <p className="text-destructive">Error loading programs</p>
        <p className="text-sm text-muted-foreground mt-2">
          {error.message}
        </p>
      </div>
    );
  }

  if (!programs || programs.length === 0) {
    return (
      <div className="text-center py-12">
        <p className="text-muted-foreground">No programs found</p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {programs.map((program) => (
        <ProgramCard key={program.program_id} program={program} />
      ))}
    </div>
  );
}


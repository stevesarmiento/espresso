'use client';

import { useProgramDetail } from '@/hooks/use-programs';
import { ProgramStats } from '@/components/programs/program-stats';
import { ProgramChart } from '@/components/programs/program-chart';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { ArrowLeft, ExternalLink } from 'lucide-react';
import Link from 'next/link';
import { Button } from '@/components/ui/button';
import { SOLANA_EXPLORER_URL } from '@/lib/constants';

interface ProgramDetailPageProps {
  params: {
    id: string;
  };
}

export default function ProgramDetailPage({ params }: ProgramDetailPageProps) {
  const { data: program, isLoading, error } = useProgramDetail(params.id, '7d');

  if (isLoading) {
    return (
      <div className="container mx-auto px-4 py-8">
        <Skeleton className="h-12 w-64 mb-8" />
        <div className="space-y-8">
          <Skeleton className="h-48 w-full" />
          <Skeleton className="h-64 w-full" />
        </div>
      </div>
    );
  }

  if (error || !program) {
    return (
      <div className="container mx-auto px-4 py-8">
        <Link href="/programs">
          <Button variant="ghost" className="mb-8">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to Programs
          </Button>
        </Link>
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-destructive">Program not found</p>
            <p className="text-sm text-muted-foreground mt-2">
              {error?.message || 'Unable to load program details'}
            </p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <Link href="/programs">
        <Button variant="ghost" className="mb-8">
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Programs
        </Button>
      </Link>

      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold mb-2">Program Details</h1>
            <code className="text-sm bg-muted px-3 py-1 rounded">
              {program.program_id}
            </code>
          </div>
          <a
            href={`${SOLANA_EXPLORER_URL}/address/${program.program_id}`}
            target="_blank"
            rel="noopener noreferrer"
          >
            <Button variant="outline">
              <ExternalLink className="mr-2 h-4 w-4" />
              View on Explorer
            </Button>
          </a>
        </div>
      </div>

      <div className="space-y-8">
        <ProgramStats program={program} />

        {program.hourly_stats && program.hourly_stats.length > 0 && (
          <ProgramChart data={program.hourly_stats} />
        )}

        {(!program.hourly_stats || program.hourly_stats.length === 0) && (
          <Card>
            <CardHeader>
              <CardTitle>Activity Chart</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">
                No historical data available for this time range
              </p>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}


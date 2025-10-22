import Link from 'next/link';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { formatNumber, formatComputeUnits, formatProgramId } from '@/lib/utils/format';
import type { ProgramStats } from '@/types';
import { ArrowUpRight } from 'lucide-react';

interface ProgramCardProps {
  program: ProgramStats;
}

export function ProgramCard({ program }: ProgramCardProps) {
  const successRate = program.success_rate;
  const badgeVariant = 
    successRate >= 95 ? 'success' : 
    successRate >= 80 ? 'warning' : 
    'destructive';

  return (
    <Link href={`/programs/${program.program_id}`}>
      <Card className="hover:shadow-lg transition-shadow cursor-pointer h-full">
        <CardHeader>
          <CardTitle className="text-lg flex items-center justify-between">
            <code className="text-sm font-mono">
              {formatProgramId(program.program_id)}
            </code>
            <ArrowUpRight className="h-4 w-4 text-muted-foreground" />
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <div className="flex justify-between items-center">
              <span className="text-sm text-muted-foreground">Transactions</span>
              <span className="font-semibold">
                {formatNumber(program.total_transactions, 0)}
              </span>
            </div>
            
            <div className="flex justify-between items-center">
              <span className="text-sm text-muted-foreground">Success Rate</span>
              <Badge variant={badgeVariant}>
                {program.success_rate.toFixed(1)}%
              </Badge>
            </div>
            
            <div className="flex justify-between items-center">
              <span className="text-sm text-muted-foreground">Avg CU</span>
              <span className="text-sm">
                {formatComputeUnits(program.avg_compute_units)}
              </span>
            </div>
            
            {program.total_errors > 0 && (
              <div className="flex justify-between items-center">
                <span className="text-sm text-muted-foreground">Errors</span>
                <span className="text-sm text-destructive">
                  {formatNumber(program.total_errors, 0)}
                </span>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </Link>
  );
}


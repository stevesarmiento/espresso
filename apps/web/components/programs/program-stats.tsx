import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import {
  formatNumber,
  formatComputeUnits,
  formatRelativeTime,
} from '@/lib/utils/format';
import type { ProgramDetail } from '@/types';

interface ProgramStatsProps {
  program: ProgramDetail;
}

export function ProgramStats({ program }: ProgramStatsProps) {
  const successRate = program.success_rate;
  const badgeVariant =
    successRate >= 95 ? 'success' : successRate >= 80 ? 'warning' : 'destructive';

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Total Transactions
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {formatNumber(program.total_transactions, 0)}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Success Rate
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2">
            <span className="text-2xl font-bold">
              {program.success_rate.toFixed(1)}%
            </span>
            <Badge variant={badgeVariant} className="ml-auto">
              {program.total_errors} errors
            </Badge>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Avg Compute Units
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {formatComputeUnits(program.avg_compute_units)}
          </div>
          {program.min_compute_units && program.max_compute_units && (
            <p className="text-xs text-muted-foreground mt-1">
              Range: {formatComputeUnits(program.min_compute_units)} -{' '}
              {formatComputeUnits(program.max_compute_units)}
            </p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Last Seen
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-lg font-semibold">
            {program.last_seen
              ? formatRelativeTime(program.last_seen)
              : 'Unknown'}
          </div>
          {program.first_seen && (
            <p className="text-xs text-muted-foreground mt-1">
              First seen {formatRelativeTime(program.first_seen)}
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}


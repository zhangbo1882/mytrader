import dayjs from 'dayjs';

export function formatDate(date: string | Date, format: string = 'YYYY-MM-DD'): string {
  return dayjs(date).format(format);
}

export function getDateRanges(): Record<string, { start: string; end: string }> {
  const now = dayjs();
  const today = now.format('YYYY-MM-DD');

  return {
    '1M': {
      start: now.subtract(1, 'month').format('YYYY-MM-DD'),
      end: today,
    },
    '3M': {
      start: now.subtract(3, 'month').format('YYYY-MM-DD'),
      end: today,
    },
    '6M': {
      start: now.subtract(6, 'month').format('YYYY-MM-DD'),
      end: today,
    },
    '1Y': {
      start: now.subtract(1, 'year').format('YYYY-MM-DD'),
      end: today,
    },
    YTD: {
      start: now.startOf('year').format('YYYY-MM-DD'),
      end: today,
    },
  };
}

export function subtractFromDate(date: string, amount: number, unit: dayjs.ManipulateType): string {
  return dayjs(date).subtract(amount, unit).format('YYYY-MM-DD');
}

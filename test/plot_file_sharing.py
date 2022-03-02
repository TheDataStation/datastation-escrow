'''
- The ratio of the figures should be 0.65, 0.5 (a bit wider than taller)
- The figures should not have a title because they'll have a caption in the paper
- Instead of solid colors in bars, it's better to use patterns, so color-blind people can see the difference.
- any form of aggregation needs error bars, and error bars on things to be compared should be computed over samples
of the same size
- The X and Y labels should be precise. You don't need to say 10KB each there, as you can explain that in the text.
Similarly, the Y axis should just say what the metric is, and nothing specific to the experiment, that's what the
text is for as well.
- The font should be as similar as Latex as matplotlib allows you to do (searching on Google someone will have
figured this out already).
- The legend is important. One has to think carefully how to name things there, as we'll use that in the text as well.
'''

import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

num_functions_list = [5, 10, 50, 100]
num_files_list = [10, 50, 100]
num_iters = 20

upload_dataset_time_by_num_functions = defaultdict(list)
create_policy_time_by_num_functions = defaultdict(list)
call_api_time_by_num_functions = defaultdict(list)

upload_dataset_std_by_num_functions = defaultdict(list)
create_policy_std_by_num_functions = defaultdict(list)
call_api_std_by_num_functions = defaultdict(list)

for num_functions in num_functions_list:

    for num_files in num_files_list:
        result_file_name = "../numbers/chameleon/file_sharing_{}_{}.npy".format(num_functions, num_files)

        print("num_functions={} num_files={}:".format(num_files, num_functions))

        result = np.load(result_file_name)

        avg_times = result.mean(axis=0)
        print(avg_times)

        upload_dataset_time_by_num_functions[num_functions].append(avg_times[0])
        create_policy_time_by_num_functions[num_functions].append(avg_times[1])
        call_api_time_by_num_functions[num_functions].append(avg_times[2])

        upload_dataset_std_by_num_functions[num_functions].append(np.std(result[:, 0]))
        create_policy_std_by_num_functions[num_functions].append(np.std(result[:, 1]))
        call_api_std_by_num_functions[num_functions].append(np.std(result[:, 2]))

# # Options
# params = {'text.usetex': True,
#           'font.size': 11,
#           'font.family': 'lmodern',
#           # 'text.latex.unicode': True
#           }
# plt.rcParams.update(params)
plt.rcParams.update({
    'font.family': 'serif',
    # "font.serif": ["Computer Modern Roman"],
    'font.size': 20,
})

# plt.rcParams['font.family'] = 'serif'
# plt.rcParams['font.serif'] = "Computer Modern Roman"
# plt.rcParams['font.size'] = 20

labels = [str(num) for num in num_files_list]

width = 0.2  # the width of the bars
x = np.arange(len(labels))  # the label locationshatches = ['\\', '-', 'o', '.']
hatches = ['\\\\', 'o', '//', '..']

fig, ax = plt.subplots(figsize=(13, 10))
for i, num_functions in enumerate(num_functions_list):
    ax.bar(x + width * i, call_api_time_by_num_functions[num_functions], width,
           yerr=call_api_std_by_num_functions[num_functions],
           label='{} functions'.format(num_functions),
           fill=False, hatch=hatches[i])

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('time(s)')
ax.set_xlabel('Number of DEs')
# ax.set_title('Average download (call_api) time over {} runs'.format(num_iters))
ax.set_xticks(x + width + width / 2, labels)
ax.legend()
fig.tight_layout()

plt.savefig("./file_sharing_plots/call_api_time",
            # dpi=500,
            bbox_inches='tight')
plt.show()
plt.close(fig)

fig, ax = plt.subplots(figsize=(13, 10))

for i, num_functions in enumerate(num_functions_list):
    if i == 0:
        ax.bar(x + width * i, upload_dataset_time_by_num_functions[num_functions], width,
               yerr=upload_dataset_std_by_num_functions[num_functions],
               label='upload dataset time'.format(num_functions),
               color='grey', hatch='xx')
    else:
        ax.bar(x + width * i, upload_dataset_time_by_num_functions[num_functions], width,
               yerr=upload_dataset_std_by_num_functions[num_functions],
               color='grey', hatch='xx')

    ax.bar(x + width * i, create_policy_time_by_num_functions[num_functions], width,
           bottom=upload_dataset_time_by_num_functions[num_functions],
           yerr=create_policy_std_by_num_functions[num_functions],
           label='{} functions'.format(num_functions),
           fill=False, hatch=hatches[i])

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('time(s)')
ax.set_xlabel('Number of DEs')
# ax.set_title('Average upload DE and create policies time over {} runs'.format(num_iters))
ax.set_xticks(x + width + width / 2, labels)
ax.legend()
fig.tight_layout()

plt.savefig("./file_sharing_plots/upload_dataset_and_create_policy_time",
            # dpi=500,
            bbox_inches='tight')
plt.show()
plt.close(fig)

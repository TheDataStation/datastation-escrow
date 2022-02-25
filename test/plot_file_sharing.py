import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

num_files_list = [1, 10, 100]
num_functions_list = [5, 10, 15]
num_iters = 10

upload_dataset_time_by_num_functions = defaultdict(list)
create_policy_time_by_num_functions = defaultdict(list)
# upload_dataset_and_create_policy_time_by_num_functions = defaultdict(list)
call_api_time_by_num_functions = defaultdict(list)

for num_files in num_files_list:

    for num_functions in num_functions_list:
        result_file_name = "../numbers/file_sharing/file_sharing_{}_{}.npy".format(num_files, num_functions)

        print("num_files={} num_functions={}:".format(num_files, num_functions))

        result = np.load(result_file_name)

        avg_times = result.mean(axis=0)
        print(avg_times)

        upload_dataset_time_by_num_functions[num_functions].append(avg_times[0])
        create_policy_time_by_num_functions[num_functions].append(avg_times[1])
        # upload_dataset_and_create_policy_time_by_num_functions[num_functions].append(avg_times[0]+avg_times[1])
        call_api_time_by_num_functions[num_functions].append(avg_times[2])

labels = [str(num) for num in num_files_list]

x = np.arange(len(labels))  # the label locations
width = 0.25  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - 0.25, call_api_time_by_num_functions[5], width,
                # yerr=np.std(call_api_time_by_num_functions[5]),
                label='num_functions=5')
rects2 = ax.bar(x, call_api_time_by_num_functions[10], width,
                # yerr=np.std(call_api_time_by_num_functions[10]),
                label='num_functions=10')
rects3 = ax.bar(x + 0.25, call_api_time_by_num_functions[15], width,
                # yerr=np.std(call_api_time_by_num_functions[15]),
                label='num_functions=15')


# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('time(s)')
ax.set_xlabel('Number of DEs, 10KB each')
ax.set_title('Average download (call_api) time over {} runs'.format(num_iters))
ax.set_xticks(x, labels)
ax.legend()
fig.tight_layout()

plt.savefig("./file_sharing_plots/call_api_time")
plt.show()
plt.close(fig)

fig, ax = plt.subplots()
ax.bar(x - 0.25, upload_dataset_time_by_num_functions[5], width,
                # yerr=np.std(upload_dataset_and_create_policy_time_by_num_functions[5]),
                label='num_functions=5, upload_dataset')
ax.bar(x - 0.25, create_policy_time_by_num_functions[5], width,
                bottom=upload_dataset_time_by_num_functions[5],
                # yerr=np.std(upload_dataset_and_create_policy_time_by_num_functions[5]),
                label='num_functions=5, create_policy')
ax.bar(x, upload_dataset_time_by_num_functions[10], width,
                # yerr=np.std(upload_dataset_and_create_policy_time_by_num_functions[10]),
                label='num_functions=10, upload_dataset')
ax.bar(x, create_policy_time_by_num_functions[10], width,
                bottom=upload_dataset_time_by_num_functions[10],
                # yerr=np.std(upload_dataset_and_create_policy_time_by_num_functions[10]),
                label='num_functions=10, create_policy')
ax.bar(x + 0.25, upload_dataset_time_by_num_functions[15], width,
                # yerr=np.std(upload_dataset_and_create_policy_time_by_num_functions[15]),
                label='num_functions=15, upload_dataset')
ax.bar(x + 0.25, create_policy_time_by_num_functions[15], width,
                bottom=upload_dataset_time_by_num_functions[15],
                # yerr=np.std(upload_dataset_and_create_policy_time_by_num_functions[15]),
                label='num_functions=15, create_policy')

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('time(s)')
ax.set_xlabel('Number of DEs, 10KB each')
ax.set_title('Average upload DE and create policies time over {} runs'.format(num_iters))
ax.set_xticks(x, labels)
ax.legend()
fig.tight_layout()

plt.savefig("./file_sharing_plots/upload_dataset_and_create_policy_time")
plt.show()
plt.close(fig)


